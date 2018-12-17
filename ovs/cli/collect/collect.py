import click


@click.command('logs')
def logs():
    #         LOGFILE=/tmp/ovs-`hostname`-`date "+%Y%m%d%H%M%S"`-logs.tar
    #         rm -f ${LOGFILE}
    #         rm -f ${LOGFILE}.gz
    #         journalctl -u ovs-* -u asd-* -u alba-* --no-pager > /var/log/journald.log 2>&1 || true
    #         touch ${LOGFILE}
    #         tar uvf ${LOGFILE} /var/log/arakoon* > /dev/null 2>&1
    #         tar uvf ${LOGFILE} /var/log/nginx* > /dev/null 2>&1
    #         tar uvf ${LOGFILE} /var/log/ovs* > /dev/null 2>&1
    #         tar uvf ${LOGFILE} /var/log/rabbitmq* > /dev/null 2>&1
    #         tar uvf ${LOGFILE} /var/log/upstart* > /dev/null 2>&1
    #         tar uvf ${LOGFILE} /var/log/*log > /dev/null 2>&1
    #         tar uvf ${LOGFILE} /var/log/dmesg* > /dev/null 2>&1
    #         gzip ${LOGFILE} > /dev/null
    #         echo ${LOGFILE}.gz
    #     else
    raise NotImplementedError #todo