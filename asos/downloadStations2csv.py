from sshtunnel import SSHTunnelForwarder
import pymysql
import pandas

hostIP=''
ruser='' 
rpw=''
myuser=''
mypw=''
myport=

tunnel = SSHTunnelForwarder(
    hostIP,
    ssh_username=ruser,
    ssh_password=rpw,
    local_bind_address=('127.0.0.1', myport),
    remote_bind_address=('127.0.0.1', 3306))   
tunnel.start()

conn=rconn = pymysql.connect(user=myuser,
                                  password=mypw,
                                  host='127.0.0.1',
                                  db='asos',
                                  port=myport)

myq='Select distinct station,lat,lon from fullASOS;'
stationLocations=pandas.read_sql(myq,conn)
stationLocations.to_csv('stationLocations.csv',index=False)
tunnel.close()