sudo -v
mvn clean package
sudo cp target/sensorsafe.war /opt/jetty/webapps
sudo service jetty restart
