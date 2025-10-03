from src.report import run_all

if __name__ == "__main__":
    results = run_all(
        dataset_path="data/DataTask.csv",
        outputs_dir="outputs/"
    )
    print("Pipeline completed. Outputs saved at:")
    for k, v in results.items():
        print(f"{k}: {v}")
