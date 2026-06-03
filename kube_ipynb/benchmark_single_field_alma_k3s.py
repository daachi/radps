"""
Single-field ALMA imaging "smoke test" / benchmark for the radps-k3s (rapdps-cv)
cluster.

This is the k3s analogue of tacc_vista_benchmarks_astroviper/benchmark_single_field_alma_final.py.
Instead of submitting a multi-node SLURM job on TACC Vista, it spins up a Dask
cluster via the dask-kubernetes Operator, pins the workers to the EPYC nodes
(ework5-8), reads a processing set from the NFS `transfer` export, runs
`image_cube_single_field`, and writes the image + a timing table to the NFS
`outputs` export.

Run it from inside a radps-hub JupyterHub notebook (the notebook pod is the
Dask client/driver; its service account `dask-jupyter-sa` already has the RBAC
to create DaskClusters in the radps-hub namespace).

    python benchmark_single_field_alma_k3s.py

Live cluster facts this script encodes (verified via kubectl exec into
jupyter-admin against the live v0.0.6 image, NOT the repo charts):
  - Dask operator runs in namespace `dask-system`, CRDs `*.kubernetes.dask.org`.
  - dask / distributed / dask_kubernetes are all 2025.7.0; KubeCluster accepts a
    `custom_cluster_spec` dict (used below).
  - Notebook/worker image: ghcr.io/casangi/radps-jupyter-notebook:v0.0.6
  - EPYC worker nodes ework5-8 carry NO distinguishing label, so we pin by
    kubernetes.io/hostname. (See EPYC_NODES / optional label approach below.)
  - PVCs in radps-hub: `nfs-transfer` (read-only) and `nfs-outputs` (writable).
  - The processing set already exists at /home/jovyan/transfer/2019.1.01463.S_v2.ps.zarr
  - The imager is `astroviper.distributed.imaging.cube_imaging_niter0` (NOT the
    `image_cube_single_field` name used in the TACC scripts) and takes
    `ps_name`/`image_name`/`grid_params` with `intents=`/`data_group=`/
    `double_precission=`/`workflow_ochestrator=` kwargs.
  - open_processing_set takes `intents=` (NOT `scan_intents=`).

############################################################################
# !!! BLOCKER as of 2026-06-03: v0.0.6 cannot run the imager yet !!!
#
# cube_imaging_niter0 internally does `from xradio.image import
# make_empty_sky_image, write_image`, and `xradio.image` fails to import
# because casatools / python-casacore / casaconfig are ALL MISSING from the
# v0.0.6 image (ModuleNotFoundError: casaconfig.config ...). graphviper,
# toolviper, and xradio.measurement_set (zarr/processing-set reading) all work
# fine — only the xradio.image path needs casacore.
#
# Fix: add `python-casacore` (or `casatools`) to the radps-jupyter-notebook
# image build and republish, then this script runs as-is. Until then the
# cluster/storage/node-pinning scaffolding below is exercisable, but the
# image_cube call will raise on import.
############################################################################
"""

import os
import time
import shutil
from datetime import datetime


# --------------------------------------------------------------------------- #
# Parameters — edit these.
# --------------------------------------------------------------------------- #

# Mount paths are identical on the notebook AND the worker pods (see the volume
# wiring in build_kube_cluster). ps_store/image_store therefore resolve the same
# everywhere — this is mandatory: the workers, not the notebook, open the data.
TRANSFER_DIR = "/home/jovyan/transfer"
OUTPUTS_DIR = "/home/jovyan/outputs"

PS_STORE = os.path.join(TRANSFER_DIR, "2019.1.01463.S_v2.ps.zarr")
IMAGE_STORE = os.path.join(OUTPUTS_DIR, "bench_2019.1.01463.S_v2.img.zarr")
RESULTS_DIR = os.path.join(OUTPUTS_DIR, "k3s_benchmark_results")

# EPYC nodes to pin workers to. No EPYC label exists on these nodes, so we match
# by hostname. If you'd rather use a label, run (you, not this script):
#   kubectl label node radps-k3s-ework{5,6,7,8} radps.nrao.edu/cpu=epyc
# then set EPYC_NODE_LABEL and the spec below switches to a nodeSelector.
EPYC_NODES = [
    "radps-k3s-ework5",
    "radps-k3s-ework6",
    "radps-k3s-ework7",
    "radps-k3s-ework8",
]
EPYC_NODE_LABEL = None  # e.g. {"radps.nrao.edu/cpu": "epyc"} to use a label instead

NAMESPACE = "radps-hub"
WORKER_IMAGE = "ghcr.io/casangi/radps-jupyter-notebook:v0.0.6"  # parity with the client
SERVICE_ACCOUNT = "dask-jupyter-sa"

# Stopgap until casacore ships in the image (see BLOCKER note above). When True,
# pip-install casacore onto every worker at runtime via a Dask plugin, so the
# imager's `from xradio.image import ...` resolves on the workers. Delete this
# (and the call in build_kube_cluster) once the image includes casacore.
BOOTSTRAP_CASACORE = True
BOOTSTRAP_PACKAGES = ["python-casacore"]   # lighter than casatools; won't fight numpy
BOOTSTRAP_PIP_OPTIONS = []                 # e.g. ["--user"] if /opt/conda isn't writable

# ework5-8 are 32 cores / ~251Gi. Mirror the TACC "multi-threaded-independent"
# mode: Dask advertises 1 thread/worker, but the imager spawns its own OMP pool.
N_WORKERS = 16                       # total across the 4 EPYC nodes (~4/node)
WORKER_CORES = 6                     # cpu request/limit per worker pod
WORKER_MEM_GB = 56                   # memory request/limit per worker pod
PROCESSING_FUNCTION_THREADS = 6      # OMP pool the imager opens inside each worker
N_CHUNKS = 256                       # frequency chunking; small for a quick test

# Test-sized image. The TACC benchmark used 11250x11250; keep it small here so
# the smoke test finishes in minutes. Bump image_size + niter to scale up.
IMAGE_SIZE = [2048, 2048]


# --------------------------------------------------------------------------- #
# Cluster construction
# --------------------------------------------------------------------------- #

def build_kube_cluster():
    """
    Create a dask-kubernetes Operator cluster whose workers are pinned to the
    EPYC nodes and mount the transfer (ro) + outputs (rw) NFS PVCs at the same
    paths the notebook uses. Returns (cluster, client).

    Analogue of launch_single_job_cluster() in the TACC version, but the
    "scheduler on the login node + srun workers" dance is replaced by the
    operator creating scheduler + worker pods for us.
    """
    from dask_kubernetes.operator import KubeCluster, make_cluster_spec
    from dask.distributed import Client

    # Real env vars (not dask.config) so the worker processes inherit the BLAS/
    # OMP thread caps — same reasoning as the TACC worker_env block.
    worker_env = {
        "OMP_NUM_THREADS": str(PROCESSING_FUNCTION_THREADS),
        "MKL_NUM_THREADS": str(PROCESSING_FUNCTION_THREADS),
        "NUMEXPR_NUM_THREADS": str(PROCESSING_FUNCTION_THREADS),
        "OPENBLAS_NUM_THREADS": str(PROCESSING_FUNCTION_THREADS),
    }

    spec = make_cluster_spec(
        name="bench-single-field",
        image=WORKER_IMAGE,
        n_workers=N_WORKERS,
        resources={
            "requests": {"cpu": str(WORKER_CORES), "memory": f"{WORKER_MEM_GB}Gi"},
            "limits": {"cpu": str(WORKER_CORES), "memory": f"{WORKER_MEM_GB}Gi"},
        },
        env=worker_env,
        # 1 Dask thread/worker; the imager opens its own OMP pool (independent mode).
        worker_command=["dask-worker", "--nthreads", "1"],
    )

    # --- pin workers to the EPYC nodes ------------------------------------- #
    if EPYC_NODE_LABEL:
        node_constraint = {"nodeSelector": EPYC_NODE_LABEL}
    else:
        node_constraint = {
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {
                                        "key": "kubernetes.io/hostname",
                                        "operator": "In",
                                        "values": EPYC_NODES,
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }

    # --- NFS volumes: workers must mount the same PVCs at the same paths ---- #
    volumes = [
        {"name": "transfer", "persistentVolumeClaim": {"claimName": "nfs-transfer", "readOnly": True}},
        {"name": "outputs", "persistentVolumeClaim": {"claimName": "nfs-outputs"}},
    ]
    volume_mounts = [
        {"name": "transfer", "mountPath": TRANSFER_DIR, "readOnly": True},
        {"name": "outputs", "mountPath": OUTPUTS_DIR},
    ]

    def patch_pod(pod_spec, mount_data):
        pod_spec.update(node_constraint)
        pod_spec["serviceAccountName"] = SERVICE_ACCOUNT
        if mount_data:
            pod_spec.setdefault("volumes", []).extend(volumes)
            container = pod_spec["containers"][0]
            container.setdefault("volumeMounts", []).extend(volume_mounts)

    # Workers do the I/O and the compute -> pin + mount data.
    patch_pod(spec["spec"]["worker"]["spec"], mount_data=True)
    # Scheduler does neither -> pin to the EPYC nodes for network locality, but
    # it doesn't need the NFS mounts. (Drop the pin if you'd rather it float.)
    patch_pod(spec["spec"]["scheduler"]["spec"], mount_data=False)

    print("Creating DaskCluster (operator will schedule scheduler + worker pods)...")
    cluster = KubeCluster(custom_cluster_spec=spec, namespace=NAMESPACE)
    client = Client(cluster)
    print("Dashboard:", client.dashboard_link)

    if BOOTSTRAP_CASACORE:
        # Stopgap: install casacore on every worker (and any that join later) so
        # `from xradio.image import ...` resolves. restart_workers=True recycles
        # the worker processes so the freshly-installed package is importable.
        from dask.distributed import PipInstall

        print("Bootstrapping casacore on workers via PipInstall:", BOOTSTRAP_PACKAGES)
        client.register_plugin(
            PipInstall(
                packages=BOOTSTRAP_PACKAGES,
                pip_options=BOOTSTRAP_PIP_OPTIONS,
                restart_workers=True,
            )
        )

    return cluster, client


def configure_dask():
    """Driver-side dask config; mirrors the benchmark's dask_config()."""
    import dask
    dask.config.set({"distributed.scheduler.work-stealing": True})
    dask.config.set({"distributed.scheduler.allowed-failures": 10})
    # Don't let Dask pause/terminate under memory pressure during a timed run.
    dask.config.set({"distributed.worker.memory.pause": False})
    dask.config.set({"distributed.worker.memory.terminate": False})
    dask.config.set({"distributed.worker.memory.target": 0.95})
    dask.config.set({"distributed.worker.memory.spill": 0.95})
    dask.config.set({"distributed.comm.timeouts.connect": "600s"})


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    from xradio.measurement_set import open_processing_set
    # Imager name + module differ from the TACC scripts (verified against v0.0.6).
    # NB: this import pulls xradio.image, which needs casacore — see the BLOCKER
    # note in the module docstring.
    from astroviper.distributed.imaging.cube_imaging_niter0 import cube_imaging_niter0
    import numpy as np
    import pandas as pd

    os.makedirs(RESULTS_DIR, exist_ok=True)
    configure_dask()

    print("1 Opening processing set:", PS_STORE)
    start = time.time()
    intents = ["OBSERVE_TARGET#ON_SOURCE"]
    ps = open_processing_set(PS_STORE, intents=intents)   # kwarg is `intents`, not `scan_intents`
    print("   opened in %.1fs" % (time.time() - start))
    print("   summary:\n", ps.xr_ps.summary())

    print("2 Building grid params")
    # cube_imaging_niter0 only needs these three keys. `phase_direction` may be
    # an int field index (the imager resolves it to that field's
    # FIELD_PHASE_CENTER) — 0 = first/center field for a single-field obs.
    # Switch to an explicit phase-center DataArray if you image a mosaic.
    grid_params = {
        "image_size": IMAGE_SIZE,
        "cell_size": np.array([-0.004, 0.004]) * np.pi / (180 * 3600),
        "phase_direction": 0,
    }

    print("3 Spinning up EPYC Dask cluster")
    cluster, client = build_kube_cluster()
    try:
        # Gate on the full worker set so the timing reflects the intended scale,
        # not partial startup (mirrors wait_for_workers in the TACC version).
        print("   waiting for %d workers..." % N_WORKERS)
        client.wait_for_workers(n_workers=N_WORKERS, timeout=900)
        print("   workers up:", len(client.scheduler_info()["workers"]))

        print("4 Imaging -> ", IMAGE_STORE)
        shutil.rmtree(IMAGE_STORE, ignore_errors=True)
        run_name = "k3s_epyc_n_workers_%d_%s" % (
            N_WORKERS,
            datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
        )
        start = time.time()
        # cube_imaging_niter0 = dirty image + PSF + primary beam, no deconvolution
        # (niter=0 is baked in) — exactly the quick smoke test we want.
        imaging_metadata = cube_imaging_niter0(
            ps_name=PS_STORE,
            image_name=IMAGE_STORE,
            grid_params=grid_params,
            n_chunks=N_CHUNKS,
            data_variables=["sky", "point_spread_function", "primary_beam"],
            intents=["OBSERVE_TARGET#ON_SOURCE"],
            data_group="base",
            double_precission=False,      # sic — that's the kwarg's spelling
            thread_info=None,
            workflow_ochestrator="dask",  # sic
        )
        imaging_time = time.time() - start
        print("   imaging finished in %.1fs" % imaging_time)
        print("   return:", imaging_metadata)

        print("5 Writing results table -> ", RESULTS_DIR)
        record = {
            "creation_date": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
            "total_time": imaging_time,
            "n_workers": N_WORKERS,
            "worker_cores": WORKER_CORES,
            "worker_mem_gb": WORKER_MEM_GB,
            "processing_function_threads": PROCESSING_FUNCTION_THREADS,
            "n_chunks": N_CHUNKS,
            "image_size": str(IMAGE_SIZE),
            "epyc_nodes": ",".join(EPYC_NODES),
            "run_name": run_name,
            "ps_store": PS_STORE,
            "image_store": IMAGE_STORE,
        }
        df = pd.DataFrame([record])
        df.to_feather(os.path.join(RESULTS_DIR, "df_" + run_name + ".ft"))

        overall_path = os.path.join(RESULTS_DIR, "df_overall_k3s.ft")
        if os.path.exists(overall_path):
            overall = pd.concat([pd.read_feather(overall_path), df], ignore_index=True)
        else:
            overall = df
        overall.to_feather(overall_path)
        print(overall)

    finally:
        # Always tear the cluster down — a leaked DaskCluster keeps worker pods
        # (and the EPYC node allocation) alive. Same intent as the TACC scancel.
        print("6 Closing cluster")
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
