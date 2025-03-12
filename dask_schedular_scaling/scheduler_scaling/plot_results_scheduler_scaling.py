import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def plot_results(csv_file):
    # Load results from CSV
    df = pd.read_csv(csv_file)

    # Plot results
    plt.figure(figsize=(10, 5))
    for min_sleep in np.unique(df["min_sleep"]):
        df_subset = df[df["min_sleep"] == min_sleep]
        max_sleep = df_subset["max_sleep"].iloc[0]
        plt.plot(
            df_subset["num_tasks"],
            df_subset["T_workflow"],
            label=f"T_workflow (min_sleep={min_sleep}, max_sleep={max_sleep})",
            marker="o",
        )
        plt.plot(
            df_subset["num_tasks"],
            df_subset["T_sum_task_times"] / df_subset["n_parallelism"],
            label=f"T_tasks / n_parallelism (min_sleep={min_sleep}, max_sleep={max_sleep})",
            marker="s",
        )

    plt.xlabel("Number of Tasks")
    plt.ylabel("Time (s)")
    # plt.xscale("log")
    # plt.yscale("log")
    plt.legend()
    # plt.title("Dask Scheduler Overhead Analysis")
    plt.grid()
    plt.savefig("scheduler_scaling_timing.png")

    plt.figure(figsize=(10, 5))
    for min_sleep in np.unique(df["min_sleep"]):
        df_subset = df[df["min_sleep"] == min_sleep]
        max_sleep = df_subset["max_sleep"].iloc[0]
        plt.plot(
            df_subset["num_tasks"],
            100
            * (
                df_subset["T_workflow"]
                - df_subset["T_sum_task_times"] / df_subset["n_parallelism"]
            )
            / df_subset["T_workflow"],
            label=f"(min_sleep={min_sleep}, max_sleep={max_sleep})",
            marker="s",
        )

    plt.xlabel("Number of Tasks")
    plt.ylabel("Percentage Overhead")
    # plt.xscale("log")
    # plt.yscale("log")
    plt.legend()
    # plt.title("Dask Scheduler Overhead Analysis")
    plt.grid()
    plt.savefig("scheduler_scaling_overhead.png")
    plt.show()


if __name__ == "__main__":
    plot_results("scheduler_scaling_results.csv")
