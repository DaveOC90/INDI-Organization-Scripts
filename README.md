INDI-Organization-Scripts
=========================

Some simple python scripts for organizing data to INDI specifications and interacting with an S3 bucket with boto based scripts

rs_org.py contains functions for converting DICOMs to NIFTI, inteacting with a COINs download and creating a more easily parsable sumlink directory, tarring data, and uploading to an S3 bucket

s3tar.py was used to pull raw data from the S3 bucket, tar it and upload the tars. This could easily be reverse engineering to download tars, extract data and upload the raw files to S3 also.

