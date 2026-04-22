import pandas as pd
 
 
def main():
    df = pd.read_feather("df_overall_dask.ft")
 
    print(df.shape)
    print()
    print(df.head(50))
    print()
    print(df.dtypes)
    
    total_time = df["total_time"].sum()
    
    import matplotlib.pyplot as plt
    plt.figure(figsize=(10, 6))
    plt.plot(df["n_nodes"], df["total_time"]/3600, marker="o")
    plt.xlabel("Number of Nodes")
    plt.ylabel("Total Time (hours)")
    plt.title("Total Time for Each Benchmark")
    plt.savefig("total_time.png")
    
    plt.figure(figsize=(10, 6))
    plt.plot(df["n_nodes"], 4*df["total_time"][4]/df["total_time"], marker="o")
    plt.xlabel("Number of Nodes")
    plt.ylabel("Speedup")
    plt.title("Speedup for Each Benchmark (relative to 4 nodes)")
    plt.savefig("speedup.png")
 
    
 
if __name__ == "__main__":
    main()