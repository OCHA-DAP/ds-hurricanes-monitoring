from src.monitoring import monitoring_utils

if __name__ == "__main__":
    monitoring_utils.update_fcast_monitoring(
        geography="all",
        clobber=True,
        disable_progress_bar=False,
    )
