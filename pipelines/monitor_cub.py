from src.email import plotting, update_emails
from src.monitoring import monitoring_utils

if __name__ == "__main__":
    monitoring_utils.update_fcast_monitoring(geography="cub")
    plotting.update_plots(fcast_obsv="fcast", geography="cub")
    update_emails.update_fcast_info_emails(geography="cub")
