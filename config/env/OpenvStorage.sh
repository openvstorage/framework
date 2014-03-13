# OpenvStorage profile: executed by Bourne-compatible login shells.

export PYTHONPATH="${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"

ovs() {
  source /opt/OpenvStorage/bin/activate
  cd /opt/OpenvStorage
  ipython
}
