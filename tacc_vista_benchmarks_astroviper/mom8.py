
 

def main():
    import xarray as xr
    import numpy as np
    from toolviper.dask.client import local_client
    scratch = "/scratch/11335/jsteeb"
    viper_client = local_client(cores=12,threads_per_worker=12, memory_limit="18GB",dask_local_dir=scratch + "/dask_scratch",dashboard_address = ":8788")
        
    
    img_xds = xr.open_zarr("/scratch/11335/jsteeb/2019.1.01463.S_v2.img.zarr",chunks="auto")
    
    print(img_xds)
    
    mom8_xds = img_xds.SKY_RESIDUAL.max(axis=[0,1],skipna=True)
    
    print(mom8_xds)
    
    mom8_xds = mom8_xds.compute()
    mom8_xds.to_zarr("/work/11335/jsteeb/vista/benchmark_scripts/2019.1.01463.S_v2.mom8.zarr", mode="w", consolidated=True)
    
 
if __name__ == "__main__":
    main()