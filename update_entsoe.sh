#!/bin/sh

logdir="/entsoe/logs/"
logfile="${logdir}/update_entsoe_$(date +'%Y_%m_%d_%H_%M').log"

main_script="/entsoe/lib_entsoe.py"
config_file_entsoe="/entsoe/config_entsoe.ini"
config_file_entsoe_data_types="/entsoe/config_entsoe_last_update.ini"
file_mapping_countries_bidding_zones="/entsoe/countries_bidding_zones.xlsx"
py="/usr/bin/python3"

${py} ${main_script} --config_file_entsoe ${config_file_entsoe} \
--config_file_entsoe_data_types ${config_file_entsoe_data_types} \
--file_mapping_countries_bidding_zones ${file_mapping_countries_bidding_zones} \
--logfile ${logfile}

# crontab -e
# Add this command line:  17 -> 19
# 10 17 * * * sh /data/jupyter_notebook_dir/database/entsoe/update_entsoe.sh >> /data/jupyter_notebook_dir/database/entsoe/out.log 2>&1
# @reboot sh /data/jupyter_notebook_dir/database/entsoe/update_entsoe.sh >> /data/jupyter_notebook_dir/database/entsoe/out.log 2>&1

# @reboot /data/anaconda/envs/py35/bin/jupyter notebook > /home/AMENERGY/out.juypter.txt 2>&1
# grep CRON /var/log/syslog