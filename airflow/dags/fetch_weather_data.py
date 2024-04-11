from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

import os
import pandas as pd
from datetime import datetime, timedelta

import logging