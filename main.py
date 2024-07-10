import sys

from lib import Utilis

from lib.logger import Log4j


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: sbdl {local,qa,prod} {load_date} : Argument are missing ")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    print(job_run_env)
    load_date = sys.argv[2]

    spark = Utilis.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Finisged Createing Spark Session")