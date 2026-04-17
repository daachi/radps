

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
    dask_config("/scratch/11335/jsteeb/dask_scratch")
    
    print('1 Logging Parameters Setup')

    log_level = "DEBUG" 
    #log_level = "INFO" 
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

    print('4')
    
    polarization_params = {}
    data_variables = ["sky", "point_spread_function", "primary_beam"]

    from datetime import datetime
    overall_benchmark_df = pd.DataFrame()
    WOP='dask'

    print('5')
    
    number_of_nodes_list =  [2]
    workers_per_node_list = [20]
    dask_local_dir = scratch + "/dask_scratch"
    n_chunks = [15360]
    
    print('6')
    
    for setup_indx in range(len(number_of_nodes_list)):
        print('Removing image')
        os.system("rm -rf " + image_name)
        print('Done removing image')
        workers_per_node=workers_per_node_list[setup_indx]
        number_of_nodes=number_of_nodes_list[setup_indx]
        memory_per_node=22*workers_per_node
        n_threads = workers_per_node*number_of_nodes
        memory_per_thread = 20.489999999292195

        print('Dask setup:',setup_indx,workers_per_node,number_of_nodes,memory_per_node,n_threads)
        viper_client = local_client(cores=12,threads_per_worker=12, memory_limit="18GB",dask_local_dir=scratch + "/dask_scratch",log_params=log_params,worker_log_params=worker_logs,dashboard_address = ":8786", resources={"slots": 1})
        #viper_client = local_client(cores=1, memory_limit="65GB",dask_local_dir=scratch + "/dask_scratch",log_params=log_params,worker_log_params=worker_logs,dashboard_address = ":8784")
        # viper_client = Client(cluster, timeout="3600s", dashboard_address = ":8786", **log_params)
        
        user_input = input("Please enter something: ")
        print("You entered: " + user_input)
        
        #assert thread_info['n_threads'] == n_threads, 'Number of threads wrong.'
        #assert np.abs((thread_info['memory_per_thread']-memory_per_thread)) < 0.00001, 'Memory per thread wrong.'

        for n_c in n_chunks:
            single_run_name = WOP+'_n_threads_'+str(n_threads)+'_n_chunks_' + str(n_c) 
            import time
            start = time.time()
            print('1.')
            print('The path:',os.path.join(uid,'v3_db_'+single_run_name+'.html'))
            #with performance_report(filename=os.path.join(uid,'v3_db_'+single_run_name+'.html')):
            print('2.')
            # intents = ["OBSERVE_TARGET#ON_SOURCE"]
            # ps = open_processing_set(ps_store, intents=intents)
            # np.sum(ps.get(0).VISIBILITY).compute()
            # return_df = pd.DataFrame()
            # return_df = cube_imaging_niter0_single_field(ps_store, image_name, grid_params, n_chunks=n_c, data_variables=data_variables, data_group='corrected')
            
            imaging_metadata_dict = image_cube_single_field( ps_store = ps_store,
                                image_store = image_name,
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
                                image_data_variables_keep=[ "sky_residual", "point_spread_function", "primary_beam"],
                                processing_set_data_group_name="base",
                                double_precision=True,
                                thread_info=None,
                                n_chunks=n_c,
                                overwrite=True,
                                processing_function_threads=12,
                            )
            
            
            
            print('3.')
            #print('1 The return dict ', type(return_df), return_df)
            imaging_time = time.time()-start

            # return_df.to_feather(os.path.join(uid,'v3_df_'+single_run_name+ '.ft'))

            # overall_dict = {'creation_date':datetime.today().strftime('%Y-%m-%d %H:%M:%S'), 'total_time':imaging_time, 
            #                 'n_threads':n_threads, 'workers_per_node':workers_per_node, 'number_of_nodes': number_of_nodes, 
            #                 'memory_per_thread': memory_per_thread, 'n_chunks':[n_c], 'run_name':single_run_name}
            # overall_benchmark_df = pd.concat([overall_benchmark_df, pd.DataFrame(overall_dict)], ignore_index=True)
            # print('overall_benchmark_df',pd.DataFrame(overall_dict))
            # overall_benchmark_df.to_feather(os.path.join(uid,'v3_df_overall_' + WOP +'.ft'))
        viper_client.shutdown()
    # print('overall_benchmark_df',overall_benchmark_df)
    # overall_benchmark_df.to_feather(os.path.join(uid,'v3_df_overall_' + WOP +'.ft'))
    
    # print(pd.read_feather(os.path.join(uid,'v3_df_overall_' + WOP +'.ft')))
    
    print('Shutting down viper client')
    
    viper_client.shutdown()
    

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
