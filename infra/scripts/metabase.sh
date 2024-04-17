## Startup script for the google compute engine for seting up metabase
## This script is used to install metabase on a google compute engine instance

# Update the package list and install the necessary packages
sudo apt-get update
sudo apt-get install -y openjdk-17-jre-headless curl

# Download the metabase jar file
curl -sL https://downloads.metabase.com/v0.49.6/metabase.jar -o metabase.jar

# Start the metabase server
java -jar metabase.jar