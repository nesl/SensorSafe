database sysmaster;
select sum(size)*2/1024 from sysextents where dbsname="sensoract"; -- in MB
