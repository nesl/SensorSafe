#/bin/bash

mvn install:install-file -Dfile=lib/ifxjdbc.jar -DgroupId=com.informix.jdbc -DartifactId=informix-jdbc-driver -Dversion=3.70JC7 -Dpackaging=jar
mvn install:install-file -Dfile=lib/ifxjdbc-javadoc.jar -DgroupId=com.informix.jdbc -DartifactId=informix-jdbc-driver -Dversion=3.70JC7 -Dpackaging=jar -Dclassifier=javadoc

mvn install:install-file -Dfile=lib/ifxjdbcx.jar -DgroupId=com.informix.jdbcx -DartifactId=informix-jdbcx-driver -Dversion=3.70JC7 -Dpackaging=jar
mvn install:install-file -Dfile=lib/ifxjdbc-javadoc.jar -DgroupId=com.informix.jdbcx -DartifactId=informix-jdbcx-driver -Dversion=3.70JC7 -Dpackaging=jar -Dclassifier=javadoc

mvn install:install-file -Dfile=lib/IfmxTimeSeries.jar -DgroupId=com.informix.timeseries -DartifactId=informix-timeseries -Dversion=5.00.FC4 -Dpackaging=jar
mvn install:install-file -Dfile=lib/IfmxTimeSeriesDoc.jar -DgroupId=com.informix.timeseries -DartifactId=informix-timeseries -Dversion=5.00.FC4 -Dpackaging=jar -Dclassifier=javadoc
