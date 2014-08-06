rm -rf cov-int/
rm zoodb.tgz
cov-build --dir cov-int ./mvn_compile.sh
tar czvf zoodb.tgz cov-int
curl --form project=tzaeschke%2Fzoodb \
  --form token=wwCHj0_rtdqfJ-samVJz4g \
  --form email=tilmann_dev@gmx.de \
  --form file=@tarball/file/location \
  --form version="0.4.4-SNAPSHOT" \
  --form description="0.4.4-SNAPSHOT" \
    https://scan.coverity.com/builds?project=tzaeschke%2Fzoodb

