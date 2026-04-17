

def main():
    # Sweeps `image_cube_single_field` across (threading_mode, n_nodes) on the
    # TACC Vista `gg` queue. Per iteration: submits ONE multi-node SLURM job
    # (see launch_single_job_cluster), waits for all workers, runs the imager,
    # writes per-run + overall feather tables, then scancels.
    from xradio.measurement_set import open_processing_set
    from astroviper.distributed.imaging.image_cube_single_field import image_cube_single_field
    import pandas as pd
    import numpy as np
    import os
    import shutil
    import subprocess
    import time
    from dask.distributed import Client, LocalCluster
    from distributed.exceptions import WorkerStartTimeoutError
    from datetime import datetime

    print('1 Logging Parameters Setup')

    #log_level = "DEBUG"
    log_level = "INFO"
    log_to_file = False
    log_params = { "logger_name": "main",
                "log_to_term": True,
                "log_level": log_level,
                "log_to_file": log_to_file,
                "log_file": "client.log",
                }

    worker_logs = { "logger_name": "worker",
                "log_to_term": True,
                "log_level": log_level,
                "log_to_file": log_to_file,
                "log_file": "client_worker.log",
                }

    print('2 Opening Processing Set')
    start = time.time()
    scratch = "/scratch/11335/jsteeb"
    ps_store = scratch + "/2019.1.01463.S_v2.ps.zarr"
    intents = ["OBSERVE_TARGET#ON_SOURCE"]
    ps = open_processing_set(ps_store, scan_intents=intents)
    print('Time to open processing set:', time.time()-start)
    start = time.time()
    print('ps.xr_ps.summary()', ps.xr_ps.summary())
    print('Time to get summary:', time.time()-start)

    image_name = scratch + "/2019.1.01463.S_v2.img.zarr"
    results_dir = "/work/11335/jsteeb/vista/RADPS/tacc_vista_benchmarks/benchmark_single_field_alma_final_results"
    os.makedirs(results_dir, exist_ok=True)

    print('3 Building Image Params')

    combined_field_and_source_xds = ps.xr_ps.get_combined_field_and_source_xds()
    center_field_name = combined_field_and_source_xds.attrs["center_field_name"]
    phase_direction = (
        combined_field_and_source_xds.FIELD_PHASE_CENTER_DIRECTION.sel(
            field_name=center_field_name
        )
    )

    image_params = {}
    image_params["image_size"] = [11250, 11250]
    image_params["cell_size"] = np.array([-0.004, 0.004]) * np.pi / (180 * 3600)
    image_params["phase_direction"] = phase_direction.values
    image_params["frequency_coords"] = ps.xr_ps.get_freq_axis().values
    image_params["polarization_coords"] = ["I","Q"]
    image_params["time_coords"] = [0]
    image_params["fft_padding"] = 1.2
    image_params["cpp_gridder"] = True

    print('4 Setting Up Benchmark Loop')

    polarization_params = {}
    data_variables = ["sky", "point_spread_function", "primary_beam"]
    WOP = 'dask'
    if not os.path.exists(os.path.join(results_dir, 'df_overall_' + WOP + '.ft')):
        overall_benchmark_df = pd.DataFrame()
    else:
        overall_benchmark_df = pd.read_feather(os.path.join(results_dir, 'df_overall_' + WOP + '.ft'))

    print('5 Configuring Cluster Parameters')

    # gg queue caps: MaxNode=32, MaxNodePU=128. MaxJobsPU=20 is irrelevant here
    # since we submit one multi-node job per scale point.
    number_of_nodes_list = [32, 28, 24, 20,16,12,8,6,4,2,1]
    dask_local_dir = scratch + "/dask_scratch"
    n_chunks = [15360]

    dashboard_port = 8780
    n_workers_per_node = 12
    memory_per_node = 240  # GB

    print('6 Starting Node Loop')

    # Three axes we want to compare:
    #   dask-controlled   : Dask owns the 12 threads per worker.
    #   single-threaded   : pure single-threaded baseline (no hidden OMP pool).
    #   independent       : 1 Dask thread per worker, but the processing
    #                       function launches its own 12-thread OMP pool.
    threading_modes = ["multi-threaded-dask-controlled", "single-threaded", "multi-threaded-independent"]

    for threading_mode in threading_modes:
        print(f"Running benchmark with threading mode: {threading_mode}")
        if threading_mode == "single-threaded":
            threads_per_worker = 1
            processing_function_threads = 1
        elif threading_mode == "multi-threaded-dask-controlled":
            threads_per_worker = 12
            processing_function_threads = 12
        elif threading_mode == "multi-threaded-independent":
            threads_per_worker = 1
            processing_function_threads = 12

        dask_config("/scratch/11335/jsteeb/dask_scratch", processing_function_threads)

        for number_of_nodes in number_of_nodes_list:
            print('Removing image')
            shutil.rmtree(image_name, ignore_errors=True)
            print('Done removing image')

            # These get exported in the sbatch body. `dask.config.set` on the
            # driver doesn't reach workers launched via srun, so we pass the
            # BLAS/OMP thread counts as real env vars.
            worker_env = {
                "OMP_NUM_THREADS": processing_function_threads,
                "MKL_NUM_THREADS": processing_function_threads,
                "NUMEXPR_NUM_THREADS": processing_function_threads,
                "OPENBLAS_NUM_THREADS": processing_function_threads,
            }
            # In `multi-threaded-independent` mode Dask only advertises 1 thread
            # per worker, but the processing function still spawns a 12-thread
            # OMP pool. If SLURM cgroups pin cpus-per-task=1, that pool is
            # crammed onto a single core. max() gives the pool room to run.
            cpus_per_task = max(threads_per_worker, processing_function_threads)

            cluster, viper_client, job_id = launch_single_job_cluster(
                number_of_nodes=number_of_nodes,
                n_workers_per_node=n_workers_per_node,
                threads_per_worker=threads_per_worker,
                cpus_per_task=cpus_per_task,
                memory_per_node_gb=memory_per_node,
                python="/work/11335/jsteeb/vista/envs/zinc/bin/python",
                local_directory=dask_local_dir,
                log_directory=dask_local_dir + "/logs",
                queue="gg",
                walltime="48:00:00",
                dashboard_port=dashboard_port,
                interface_scheduler="ibp1s0",
                interface_worker="ib0",
                env_vars=worker_env,
            )
            # try/finally guarantees the SLURM job is cancelled and local
            # scheduler is closed even if wait_for_workers times out or the
            # imager raises mid-run. Without this a stuck 32-node job keeps
            # burning allocation.
            try:
                print("**************")
                print(viper_client.dashboard_link)
                # Gate the benchmark on the full worker set — we want gang
                # scheduling, not partial-startup progress.
                try:
                    viper_client.wait_for_workers(
                        n_workers=n_workers_per_node * number_of_nodes,
                        timeout=3600,
                    )
                except WorkerStartTimeoutError as err:
                    # One scale point with a laggy worker shouldn't kill the
                    # whole sweep — log and skip to the next iteration.
                    print(f"Skipping {number_of_nodes}-node run: {err}")
                    continue
                print("total number of workers ", str(n_workers_per_node * number_of_nodes))
                print("**************")

                for n_c in n_chunks:
                    single_run_name = WOP + '_n_nodes_' + str(number_of_nodes) + '_n_chunks_' + str(n_c) + '_' + threading_mode + '_' + datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
                    start = time.time()

                    imaging_metadata_dict = image_cube_single_field(
                        ps_store=ps_store,
                        image_store=image_name,
                        image_params=image_params,
                        imaging_weights_params={
                            "weighting": "briggs",
                            "robust": 0.5,
                        },
                        # imaging_weights_params={
                        #     "weighting": "natural",
                        # },
                        iteration_control_params={
                            "niter": 0,
                            "nmajor": 0,
                            "threshold": 0.0,
                            "gain": 0.1,
                            "cyclefactor": 1.5,
                            "cycleniter": 10,
                            "fft_padding": 1.2,
                        },
                        gridder="prolate_spheroidal",
                        deconvolver="hogbom",
                        scan_intents="OBSERVE_TARGET#ON_SOURCE",
                        #image_data_variables_keep=["sky", "point_spread_function", "primary_beam"],
                        #image_data_variables_keep=["sky_model", "sky_residual", "sky_deconvolved", "point_spread_function", "primary_beam"],
                        image_data_variables_keep=["sky_residual", "point_spread_function", "primary_beam"],
                        processing_set_data_group_name="base",
                        double_precision=True,
                        thread_info=None,
                        n_chunks=n_c,
                        overwrite=True,
                        processing_function_threads=processing_function_threads,
                    )

                    print('The return dict:', imaging_metadata_dict)
                    imaging_time = time.time() - start

                    # Convert to DataFrame if image_cube_single_field returns a dict
                    if isinstance(imaging_metadata_dict, dict):
                        imaging_metadata_df = pd.DataFrame([imaging_metadata_dict])
                    else:
                        imaging_metadata_df = imaging_metadata_dict
                    imaging_metadata_df.to_feather(os.path.join(results_dir, 'df_' + single_run_name  +  '.ft'))

                    overall_dict = {
                        'creation_date': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                        'total_time': imaging_time,
                        'n_nodes': number_of_nodes,
                        'n_workers_per_node': n_workers_per_node,
                        'n_threads_per_worker': threads_per_worker,
                        'processing_function_threads': processing_function_threads,
                        'memory_per_node': memory_per_node,
                        'n_chunks': [n_c],
                        'run_name': single_run_name,
                        'threading_mode': threading_mode,
                    }
                    overall_benchmark_df = pd.concat([overall_benchmark_df, pd.DataFrame(overall_dict)], ignore_index=True)
                    print('overall_benchmark_df', pd.DataFrame(overall_dict))
                    overall_benchmark_df.to_feather(os.path.join(results_dir, 'df_overall_' + WOP + '.ft'))

            finally:
                subprocess.run(["scancel", job_id], check=False)
                viper_client.close()
                cluster.close()

    print('overall_benchmark_df', overall_benchmark_df)
    overall_benchmark_df.to_feather(os.path.join(results_dir, 'df_overall_' + WOP + '.ft'))

    print(pd.read_feather(os.path.join(results_dir, 'df_overall_' + WOP + '.ft')))



def launch_single_job_cluster(
    number_of_nodes,
    n_workers_per_node,
    threads_per_worker,
    memory_per_node_gb,
    python,
    local_directory,
    log_directory,
    cpus_per_task=None,
    env_vars=None,
    queue="gg",
    walltime="48:00:00",
    dashboard_port=8780,
    interface_scheduler="ibp1s0",
    interface_worker="ib0",
):
    # dask-jobqueue's SLURMCluster submits one SLURM job per group of
    # `processes` workers, so scaling to N nodes means N jobs — which hits
    # MaxJobsPU=20 on gg. Here we submit a single multi-node sbatch and use
    # srun to fan n_workers_per_node dask-worker processes across all nodes.
    # The scheduler runs in-process on the driver (login-side IB interface).
    import os
    import re
    import shlex
    import subprocess
    import uuid
    from dask.distributed import Client, LocalCluster

    if cpus_per_task is None:
        cpus_per_task = threads_per_worker

    os.makedirs(log_directory, exist_ok=True)

    # n_workers=0: LocalCluster is used here purely as a scheduler host.
    # Workers attach later via the SLURM job below.
    # Vista note: login nodes expose IB as `ibp1s0`, compute nodes as `ib0`.
    cluster = LocalCluster(
        n_workers=0,
        interface=interface_scheduler,
        dashboard_address=f":{dashboard_port}",
    )
    client = Client(cluster)
    scheduler_addr = cluster.scheduler_address

    mem_per_worker_gb = memory_per_node_gb // n_workers_per_node
    n_tasks = number_of_nodes * n_workers_per_node

    # Real env vars (not dask.config) so the srun-launched worker processes
    # actually inherit them.
    env_exports = ""
    if env_vars:
        env_exports = "\n".join(
            f"export {k}={shlex.quote(str(v))}" for k, v in env_vars.items()
        ) + "\n"

    # Each srun task runs this helper, where ${{SLURM_PROCID}} is expanded
    # per-task. Without a unique --name, every task on a node defaults to the
    # same hostname-based name, the scheduler treats subsequent registrations
    # as the same worker reconnecting, and the workers flap in a loop until
    # death-timeout kicks one out.
    job_tag = uuid.uuid4().hex[:8]
    worker_script_path = os.path.join(log_directory, f"dask_worker_{job_tag}.sh")
    worker_script = f"""#!/usr/bin/env bash
exec {python} -m distributed.cli.dask_worker {scheduler_addr} \\
    --name "viper-${{SLURM_PROCID}}-$(hostname -s)" \\
    --nthreads {threads_per_worker} --nworkers 1 \\
    --memory-limit {mem_per_worker_gb}GB \\
    --local-directory {local_directory} \\
    --interface {interface_worker} \\
    --resources "slots=1" --nanny --death-timeout 300
"""
    with open(worker_script_path, "w") as f:
        f.write(worker_script)
    os.chmod(worker_script_path, 0o755)

    sbatch_body = f"""#!/usr/bin/env bash
#SBATCH -J dask-worker
#SBATCH -o {log_directory}/dask-worker-%J.out
#SBATCH -e {log_directory}/dask-worker-%J.err
#SBATCH -p {queue}
#SBATCH -N {number_of_nodes}
#SBATCH --ntasks-per-node={n_workers_per_node}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --mem=0
#SBATCH -t {walltime}

{env_exports}
srun --ntasks={n_tasks} --ntasks-per-node={n_workers_per_node} \\
     --cpus-per-task={cpus_per_task} \\
    {worker_script_path}
"""
    script_path = os.path.join(log_directory, f"dask_{job_tag}.sbatch")
    with open(script_path, "w") as f:
        f.write(sbatch_body)

    print(sbatch_body)
    print("**************")
    # TACC's submit hooks prepend a welcome banner to sbatch stdout, so the
    # `--parsable` JOBID[;CLUSTER] line is not the whole output. Scan every
    # line for the parsable pattern and take the last match.
    raw_out = subprocess.check_output(
        ["sbatch", "--parsable", script_path],
        stderr=subprocess.STDOUT,
        text=True,
    )
    matches = re.findall(r"^\s*(\d+)(?:;\S+)?\s*$", raw_out, flags=re.MULTILINE)
    if not matches:
        raise RuntimeError(f"Could not parse job id from sbatch output:\n{raw_out}")
    job_id = matches[-1]
    print(f"Submitted SLURM job {job_id} for {n_tasks} workers on {number_of_nodes} nodes")
    return cluster, client, job_id


def dask_config(local_directory, n_threads):
    # Applies to the in-process scheduler only. The nanny.environ.* entries
    # are a belt-and-suspenders companion to the env vars exported by the
    # sbatch body — they take effect only if the worker process reads this
    # driver's config (e.g. via dask config files), not automatically.
    import dask
    if local_directory:
        dask.config.set({"temporary_directory": local_directory})

    dask.config.set({"distributed.scheduler.allowed-failures": 10})
    dask.config.set({"distributed.scheduler.work-stealing": True})
    dask.config.set({"distributed.scheduler.unknown-task-duration": "99m"})
    # Disable pause/terminate so benchmark measurements aren't perturbed by
    # Dask backing off under memory pressure.
    dask.config.set({"distributed.worker.memory.pause": False})
    dask.config.set({"distributed.worker.memory.terminate": False})
    dask.config.set({"distributed.worker.memory.target": 0.95})
    dask.config.set({"distributed.worker.memory.spill": 0.9})
    # Long timeouts so transient IB hiccups don't kill a scale run.
    dask.config.set({"distributed.comm.timeouts.connect": "3600s"})
    dask.config.set({"distributed.comm.timeouts.tcp": "3600s"})
    dask.config.set({"distributed.nanny.environ.OMP_NUM_THREADS": n_threads})
    dask.config.set({"distributed.nanny.environ.MKL_NUM_THREADS": n_threads})
    dask.config.set({"distributed.nanny.environ.NUMEXPR_NUM_THREADS": n_threads})
    dask.config.set({"distributed.nanny.environ.OPENBLAS_NUM_THREADS": n_threads})
    dask.config.set({"distributed.nanny.environ.PARALLEL_NUM_THREADS": n_threads})
    dask.config.set({"distributed.nanny.environ.DASK_INTERNAL_THREADS": n_threads})



if __name__ == "__main__":
    main()



#TACC Vista notes:
# alias sinfo2='sinfo -ao '\''%20P %.5a %.10l %.6D %.6t'\'''
# alias sinfo3='echo A/I/O/T: Allocated/Idle/Other/Total; sinfo -ao "%20P %5a %.10l %16F"'


"""
#!/usr/bin/env bash

#SBATCH -J dask-worker
#SBATCH -e /scratch/11335/jsteeb/dask_scratch/logs/dask-worker-%J.err
#SBATCH -o /scratch/11335/jsteeb/dask_scratch/logs/dask-worker-%J.out
#SBATCH -p gg
#SBATCH -n 1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH -t 24:00:00
#SBATCH -N 1

/work/11335/jsteeb/vista/envs/zinc/bin/python -m distributed.cli.dask_worker tcp://192.168.16.11:39705 --name dummy-name --nthreads 12 --memory-limit 18.63GiB --nworkers 12 --nanny --death-timeout 60 --local-directory /scratch/11335/jsteeb/dask_scratch --resources slots=1 --interface ib0
"""


# Name             MinNode  MaxNode     MaxWall  MaxNodePU  MaxJobsPU   MaxSubmit
# gg                     1       32  2-00:00:00        128         20          40
# gh                     1       64  2-00:00:00        192         20          40
# gh-dev                 1        8    02:00:00          8          1           3

# ----------------------------------------Description----------------------------
# Name:      Name of the queue/partition
# MinNode:   Minimum number of nodes allowed for each job
# MaxNode:   Maximum number of nodes allowed for each job
# MaxWall:   Maximum wall clock time for jobs running with this queue
# MaxNodePU: Maximum number of nodes each user is allowed to run at one time
# MaxJobsPU: Maximum number of jobs each user is allowed to run at one time
# MaxSubmit: Maximum number of jobs each user is allowed to submit at one time
# -------------------------------------------------------------------------------

# showq
# https://tacc.utexas.edu/research/tacc-research/showq/