import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def plot_overhead_as_function_of_n_tasks(csv_file, title, run_ids, labels, save_as, skiprows=[]):
    if skiprows:
        exclude_headerrow = ''
        if 0 in skiprows:
            skiprows = skiprows.remove(0)
            exclude_headerrow = ' row 0 (header) is excluded.'
        print(f'Skipping rows: {skiprows} {exclude_headerrow}')
    df_all = pd.read_csv(csv_file, skiprows = skiprows)
    print(df_all)
    
    
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, run_id in enumerate(run_ids):
        print(f"Processing run_id: {run_id}")
        df = df_all[df_all["run_id"]==run_id]
        print(f'{len(df)} rows selected from {csv_file}')
        
        workflow_orchestration_framework = np.unique(df["workflow_orchestration_framework"])
        n_processes = np.unique(df["n_processes"])
        n_threads_per_process = np.unique(df["n_threads_per_process"])
        label = workflow_orchestration_framework + " n_proc " + str(n_processes) + " n_thr " + str(n_threads_per_process) + " " + labels[i]
    
        ax.plot(
            df["n_tasks"],
            df["overhead_percentage"],
            label=label,
            marker="o",
        )
        ax.set_xlabel("Number of Tasks (s)")
        ax.set_ylabel("Percentage Overhead Per Task")
        ax.legend()
    
    plt.title(title)
    plt.grid()
    plt.savefig(save_as)


def plot_overhead_as_function_of_return_size(csv_file, title, run_ids, labels, save_as, skiprows=[], ):
    if skiprows:
        exclude_headerrow = ''
        if 0 in skiprows:
            skiprows = skiprows.remove(0)
            exclude_headerrow = ' row 0 (header) is excluded.'
        print(f'Skipping rows: {skiprows} {exclude_headerrow}')
    df_all = pd.read_csv(csv_file, skiprows = skiprows)
    print(df_all)
    
    
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, run_id in enumerate(run_ids):
        
        print(f"Processing run_id: {run_id}")
        df_sub = df_all[df_all["run_id"]==run_id]
        
        max_sleeps = np.unique(df_sub['t_max_sleep'])
        
        for sl in max_sleeps:
            df = df_sub[df_sub['t_max_sleep'] == sl]
            print(f'{len(df)} rows selected from {csv_file}')
            
            workflow_orchestration_framework = np.unique(df["workflow_orchestration_framework"])
            n_processes = np.unique(df["n_processes"])
            n_threads_per_process = np.unique(df["n_threads_per_process"])
            avg_sleep = (df["t_max_sleep"].iloc[0] + df["t_min_sleep"].iloc[0]) / 2
            label = workflow_orchestration_framework + " avg sleep " + str(avg_sleep) + labels[i]
            
        
            ax.plot(
                df["return_size_mb"],
                df["overhead_percentage"],
                label=label,
                marker="o",
            )
            ax.set_xlabel("Return Size (MB)")
            ax.set_ylabel("Percentage Overhead Per Task")
            ax.legend()
    
    plt.title(title)
    plt.grid()
    plt.savefig(save_as)




def plot_overhead_as_function_of_n_tasks_v2(csv_file, title, run_ids, labels, save_as, skiprows=[]):
    if skiprows:
        exclude_headerrow = ''
        if 0 in skiprows:
            skiprows = skiprows.remove(0)
            exclude_headerrow = ' row 0 (header) is excluded.'
        print(f'Skipping rows: {skiprows} {exclude_headerrow}')
    df_all = pd.read_csv(csv_file, skiprows = skiprows)
    print(df_all)
    
    
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, run_id in enumerate(run_ids):
        print(f"Processing run_id: {run_id}")
        df = df_all[df_all["run_id"]==run_id]
        print(df['t_max_sleep'])
        df = df[df['t_max_sleep'] == 4.0]
        workflow_orchestration_framework = np.unique(df["workflow_orchestration_framework"])
    
        print(f"Workflow orchestration framework: {workflow_orchestration_framework}")
        # if workflow_orchestration_framework == "prefect":
        #     df = df[df['return_size_mb'] == 10.0]
        if workflow_orchestration_framework == "prefect":
            df = df[df['return_size_mb'] == 0.1]
        print(f'{len(df)} rows selected from {csv_file}')
        

        n_processes = np.unique(df["n_processes"])
        n_threads_per_process = np.unique(df["n_threads_per_process"])
        label = workflow_orchestration_framework + " n_proc " + str(n_processes) + " n_thr " + str(n_threads_per_process) + " " + labels[i]
    
        ax.plot(
            df["n_tasks"],
            df["overhead_percentage"],
            label=label,
            marker="o",
        )
        ax.set_xlabel("Number of Tasks (s)")
        ax.set_ylabel("Percentage Overhead Per Task")
        ax.legend()
    
    plt.title(title)
    plt.grid()
    plt.savefig(save_as)


if __name__ == "__main__":

    #Plot increasing task time
    #plot_overhead_as_function_of_n_tasks("results/benchmark_return_results.csv",title="Effect of Tasks Duration on Overhead",run_ids=["jsteeb_2025-05-07_14-46-13","jsteeb_2025-06-10_05-27-44"],labels=["Avg Sleep 0.55s","Avg Sleep 5s"],save_as="plots/fig1_task_duration_overhead.png")
    
    #Return Size
    #plot_overhead_as_function_of_return_size("results/benchmark_return_results.csv",title="Effect of Return Size on Overhead",run_ids=["jsteeb_2025-05-17_10-31-01"],labels=[""],save_as="plots/fig2_return_size_overhead.png")

    #Plot stasks increasing
    #plot_overhead_as_function_of_n_tasks("results/benchmark_return_results.csv",title="Overhead as a Function of the Number of Tasks (T_avg 5s)",run_ids=["jsteeb_2025-06-07_14-06-55","jsteeb_2025-06-06_08-44-43","jsteeb_2025-06-10_05-27-44"],labels=["","",""],save_as="plots/fig3_num_tasks_overhead.png")
    
    plot_overhead_as_function_of_n_tasks_v2("results/benchmark_return_results.csv",title="Overhead as a Function of the Number of Tasks (T_avg 5s)",run_ids=["jsteeb_2025-06-07_14-06-55","jsteeb_2025-06-06_08-44-43","jsteeb_2025-06-20_09-37-19","jsteeb_2025-07-08_14-25-52"],labels=["","","",""],save_as="plots/fig6_num_tasks_overhead.png")
    

