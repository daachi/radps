

def main():

    #mamba install astroviper
    #mamba install python-casacore
    

    from toolviper.dask.client import local_client, slurm_cluster_client
    from astroviper.utils.data_partitioning import get_thread_info
    from xradio.measurement_set import open_processing_set
    from astroviper.distributed.imaging.image_cube_single_field import image_cube_single_field
    import pyarrow as pa
    import pandas as pd
    from dask.distributed import performance_report
    import numpy as np
    import os
    import time
    import dask
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster
    from datetime import datetime

    dask_config("/scratch/11335/jsteeb/dask_scratch")

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
    uid = "/work/11335/jsteeb/vista/benchmark_scripts"

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

    overall_benchmark_df = pd.DataFrame()
    WOP = 'dask'

    print('5 Configuring Cluster Parameters')

    number_of_nodes_list = [15, 10]
    dask_local_dir = scratch + "/dask_scratch"
    n_chunks = [15360]

    dashboard_port = 8780
    n_workers_per_node = 12
    threads_per_worker = 12
    memory_per_node = 240  # GB

    print('6 Starting Node Loop')

    for number_of_nodes in number_of_nodes_list:
        print('Removing image')
        os.system("rm -rf " + image_name)
        print('Done removing image')

        cluster = SLURMCluster(
            processes=n_workers_per_node,
            #cores=n_workers_per_node * threads_per_worker,
            cores=n_workers_per_node,
            interface='ib0',
            memory=f"{memory_per_node}GB",
            job_mem="0",                # suppresses the #SBATCH --mem line
            walltime="24:00:00",
            queue="gg",
            name="viper",
            python="/work/11335/jsteeb/vista/envs/zinc/bin/python",
            local_directory=dask_local_dir,
            log_directory=dask_local_dir + "/logs",
            #job_extra_directives=["--exclude=" + exclude_nodes],
            scheduler_options={"dashboard_address": ":" + str(dashboard_port), "interface": "ibp1s0"},
            worker_extra_args=["--resources", "slots=1"],
            job_extra_directives=["-N 1"],
        )

        print(cluster.job_script())
        print("**************")
        viper_client = Client(cluster)
        print(viper_client.dashboard_link)
        cluster.scale(n_workers_per_node * number_of_nodes)
        viper_client.wait_for_workers(n_workers=n_workers_per_node * number_of_nodes)
        print("**************")
        print(cluster.job_script())

        for n_c in n_chunks:
            single_run_name = WOP + '_n_nodes_' + str(number_of_nodes) + '_n_chunks_' + str(n_c)
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
                processing_function_threads=12,
            )

            print('The return dict:', imaging_metadata_dict)
            imaging_time = time.time() - start

            # Convert to DataFrame if image_cube_single_field returns a dict
            if isinstance(imaging_metadata_dict, dict):
                imaging_metadata_df = pd.DataFrame([imaging_metadata_dict])
            else:
                imaging_metadata_df = imaging_metadata_dict
            imaging_metadata_df.to_feather(os.path.join(uid, 'df_' + single_run_name + '.ft'))

            overall_dict = {
                'creation_date': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                'total_time': imaging_time,
                'n_nodes': number_of_nodes,
                'n_workers_per_node': n_workers_per_node,
                'n_threads_per_worker': threads_per_worker,
                'memory_per_node': memory_per_node,
                'n_chunks': [n_c],
                'run_name': single_run_name,
            }
            overall_benchmark_df = pd.concat([overall_benchmark_df, pd.DataFrame(overall_dict)], ignore_index=True)
            print('overall_benchmark_df', pd.DataFrame(overall_dict))
            overall_benchmark_df.to_feather(os.path.join(uid, 'df_overall_' + WOP + '.ft'))

        viper_client.shutdown()
        cluster.close()

    print('overall_benchmark_df', overall_benchmark_df)
    overall_benchmark_df.to_feather(os.path.join(uid, 'df_overall_' + WOP + '.ft'))

    print(pd.read_feather(os.path.join(uid, 'df_overall_' + WOP + '.ft')))



def dask_config(local_directory):
    import dask
    if local_directory:
        dask.config.set({"temporary_directory": local_directory})

    dask.config.set({"distributed.scheduler.allowed-failures": 10})
    dask.config.set({"distributed.scheduler.work-stealing": True})
    dask.config.set({"distributed.scheduler.unknown-task-duration": "99m"})
    dask.config.set({"distributed.worker.memory.pause": False})
    dask.config.set({"distributed.worker.memory.terminate": False})
    # dask.config.set({"distributed.worker.memory.recent-to-old-time": "999s"})
    dask.config.set({"distributed.comm.timeouts.connect": "3600s"})
    dask.config.set({"distributed.comm.timeouts.tcp": "3600s"})
    dask.config.set({"distributed.nanny.environ.OMP_NUM_THREADS": 12})
    dask.config.set({"distributed.nanny.environ.MKL_NUM_THREADS": 12})



if __name__ == "__main__":
    main()



#TACC Vista notes:
# alias sinfo2='sinfo -ao '\''%20P %.5a %.10l %.6D %.6t'\'''
# alias sinfo3='echo A/I/O/T: Allocated/Idle/Other/Total; sinfo -ao "%20P %5a %.10l %16F"'
