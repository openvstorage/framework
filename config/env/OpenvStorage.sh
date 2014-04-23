# OpenvStorage profile: executed by Bourne-compatible login shells.

export PYTHONPATH="${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"
export DISPLAY=:0

ovs() {
  source /opt/OpenvStorage/bin/activate
  cd /opt/OpenvStorage
  ipython
}
