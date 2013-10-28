database sysmaster;
select sum(size)*2/1024 from sysextents where dbsname="nesl_xively_data"; -- in MB
