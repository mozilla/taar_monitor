from decouple import config
from taar_monitor.dataloader import update_install_events
from taar_monitor.dataloader import update_ensemble_suggestions

update_install_events(spark, num_days=180)
update_ensemble_suggestions(spark, num_days=180)
