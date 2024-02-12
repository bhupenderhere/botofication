# Importing required libraries
import time
import boto3
from icecream import ic
from pyathena import connect
from pyathena.cursor import DictCursor
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import (
    ClientError,
    NoRegionError,
    NoCredentialsError,
    ParamValidationError,
    EndpointConnectionError,
    PartialCredentialsError,
)


class AthenaConnector:
    def __init__(self, *args, **kwargs):
        self.__args = self.__get_args(*args, **kwargs)
        self.__region = self.__args.get("region")
        self.__database = self.__args.get("database")
        self.__workgroup = self.__args.get("workgroup")
        self.__data_catalog = self.__args.get("data_catalog")
        self.__output_bucket = self.__args.get("output_bucket")
        self.__output_location = self.__args.get("output_location")
        self.__aws_access_key_id = self.__args.get("aws_access_key_id")
        self.__aws_secret_access_key = self.__args.get("aws_secret_access_key")
        self.__client = (
            boto3.client(
                "athena",
                region_name=self.__region,
                aws_access_key_id=self.__aws_access_key_id,
                aws_secret_access_key=self.__aws_secret_access_key,
            )
            if self.__aws_access_key_id and self.__aws_secret_access_key
            else boto3.client("athena", region_name=self.__region)
        )

    @property
    def region(self) -> str:
        """
        The AWS region

        Returns:
            `str` : The AWS region
        """
        return self.__region

    @property
    def database(self) -> str:
        """
        The database name

        Returns:
            `str` : The database name
        """
        if not self.__database:
            raise ValueError("Database Name Not Provided!")
        return self.__database

    @database.setter
    def database(self, database: str) -> None:
        """
        Set the database name

        Args:
            `database (str)` : The database name
        """
        if not isinstance(database, str):
            e = f"`database` -> Expected String Or Bytes-Like Object"
            raise TypeError(e)
        self.__database = database

    @property
    def output_bucket(self) -> str:
        """
        The output bucket name

        Returns:
            `str` : The output bucket name
        """
        if not self.__output_bucket:
            raise ValueError("Output Bucket Not Provided!")
        return self.__output_bucket

    @output_bucket.setter
    def output_bucket(self, output_bucket: str) -> None:
        """
        Set the output bucket name

        Args:
            `output_bucket (str)` : The output bucket name
        """
        if not isinstance(output_bucket, str):
            e = f"`output_bucket` -> Expected String Or Bytes-Like Object"
            raise TypeError(e)
        self.__output_bucket = output_bucket

    @property
    def output_location(self) -> str:
        """
        The output location

        Returns:
            `str` : The output location
        """
        if not self.__output_location:
            raise ValueError("Output Location Not Provided!")
        return self.__output_location

    @output_location.setter
    def output_location(self, output_location: str) -> None:
        """
        Set the output location

        Args:
            `output_location (str)` : The output location
        """
        if not isinstance(output_location, str):
            e = f"`output_location` -> Expected String Or Bytes-Like Object"
            raise TypeError(e)
        self.__output_location = output_location

    @property
    def workgroup(self) -> str:
        """
        The workgroup name

        Returns:
            `str` : The workgroup name
        """
        if not self.__workgroup:
            raise ValueError("Workgroup Not Provided!")
        return self.__workgroup

    @workgroup.setter
    def workgroup(self, workgroup: str) -> None:
        """
        Set the workgroup name

        Args:
            `workgroup (str)` : The workgroup name
        """
        if not isinstance(workgroup, str):
            e = f"`workgroup` -> Expected String Or Bytes-Like Object"
            raise TypeError(e)
        self.__workgroup = workgroup

    @property
    def data_catalog(self) -> str:
        """
        The data catalog name

        Returns:
            `str` : The data catalog name
        """
        if not self.__data_catalog:
            raise ValueError("Data Catalog Not Provided!")
        return self.__data_catalog

    @data_catalog.setter
    def data_catalog(self, data_catalog: str) -> None:
        """
        Set the data catalog name

        Args:
            `data_catalog (str)` : The data catalog name
        """
        if not isinstance(data_catalog, str):
            e = f"`data_catalog` -> Expected String Or Bytes-Like Object"
            raise TypeError(e)
        self.__data_catalog = data_catalog

    @property
    def workgroups(self) -> list[str]:
        """
        Get the workgroups

        Returns:
            `list[str]` : The workgroups
        """
        try:
            workgroups: dict = self.__client.list_work_groups()
            workgroups_list: list = [
                workgroup["Name"] for workgroup in workgroups["WorkGroups"]
            ]
            return workgroups_list
        except (
            ClientError,
            ParamValidationError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
            NoRegionError,
            Exception,
        ) as e:
            raise Exception(f"Error Getting Workgroups: {e}")

    @property
    def data_catalogs(self) -> list[str]:
        """
        Get the data catalogs

        Returns:
            `list[str]` : The data catalogs
        """
        try:
            data_catalogs: dict = self.__client.list_data_catalogs()
            data_catalogs_list: list = [
                catalog["CatalogName"]
                for catalog in data_catalogs["DataCatalogsSummary"]
            ]
            return data_catalogs_list
        except (
            ClientError,
            ParamValidationError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
            NoRegionError,
            Exception,
        ) as e:
            raise Exception(f"Error Getting Data Catalogs: {e}")

    def get_saved_queries_ids(self, workgroup: str = None) -> list[str]:
        """
        Get the saved queries ids

        Args:
            `workgroup (str)` : The workgroup name

        Returns:
            `list[str]` : The saved queries ids
        """
        try:
            if self.__workgroup or workgroup:
                saved_queries: dict = self.__client.list_named_queries(
                    WorkGroup=workgroup or self.__workgroup
                )
                query_ids = saved_queries["NamedQueryIds"]
                return query_ids
            else:
                raise ValueError("Workgroup Not Provided!")
        except (
            ClientError,
            ParamValidationError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
            NoRegionError,
            Exception,
        ) as e:
            raise Exception(f"Error Getting Saved Queries: {e}")

    def get_saved_queries(self, workgroup: str = None) -> list[dict]:
        """
        Get the saved queries

        Args:
            `workgroup (str)` : The workgroup name

        Returns:
            `list[dict]` : The saved queries
        """
        try:
            if self.__workgroup or workgroup:
                saved_queries: dict = self.__client.list_named_queries(
                    WorkGroup=workgroup or self.__workgroup
                )
                query_ids = saved_queries["NamedQueryIds"]

                def __get_query(query_id):
                    query = self.__client.get_named_query(NamedQueryId=query_id)
                    return query

                with ThreadPoolExecutor() as executor:
                    saved_queries_list = list(executor.map(__get_query, query_ids))

                data = [data["NamedQuery"] for data in saved_queries_list]

                return data
            else:
                raise ValueError("Workgroup Not Provided!")
        except (
            ClientError,
            ParamValidationError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
            NoRegionError,
            Exception,
        ) as e:
            raise Exception(f"Error Getting Saved Queries: {e}")

    def get_saved_query(self, query_id: str) -> dict[str, str]:
        """
        Get the saved query

        Args:
            `query_id (str)` : The query id

        Returns:
            `dict[str, str]` : The saved query
        """
        try:
            if not query_id:
                raise ValueError("Query Id Not Provided!")
            query = self.__client.get_named_query(NamedQueryId=query_id)
            return query
        except (
            ClientError,
            ParamValidationError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
            NoRegionError,
            Exception,
        ) as e:
            raise Exception(f"Error Getting Saved Query: {e}")

    def query(self, query: str = None) -> dict:
        """
        Execute a query

        Args:
            `query (str)` : The query

        Returns:
            `dict` : The query response
        """
        with connect(
            cursor_class=DictCursor,
            region_name=self.__region,
            s3_staging_dir=f"s3://{self.__output_bucket}/{self.__output_location}",
        ) as conn:
            try:
                cur = conn.cursor()
                cur.execute(query)
                return cur.fetchall()
            except Exception as e:
                raise Exception(f"Error: {e}")

    def status_query(self, query: str) -> list[list[str]]:
        """
        Execute a query and get the status

        Args:
            `query (str)` : The query

        Returns:
            `list[list[str]]` : The query output
        """
        try:
            response = self.__client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.__database},
                ResultConfiguration={
                    "OutputLocation": f"s3://{self.__output_bucket}/{self.__output_location}",
                },
            )
            query_execution_id = response["QueryExecutionId"]

            while True:
                query_response = self.__client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                state = query_response["QueryExecution"]["Status"]["State"]

                if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    ic(f"Query execution completed with state: {state}")
                    break

                ic(f"Query execution is still in progress. Current state: {state}")
                time.sleep(5)

            result_response = self.__client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            result_data = result_response["ResultSet"]["Rows"]

            def process_data(data):
                new_data = []
                for d in data:
                    if isinstance(d, dict):
                        dict_data = d.get("Data", [])
                        values = [
                            each_dict.get("VarCharValue", None)
                            for each_dict in dict_data
                        ]
                        if len(values) > 1:
                            new_data.append(values)
                return new_data

            output = process_data(result_data)
            return output
        except (
            ClientError,
            ParamValidationError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
            NoRegionError,
            Exception,
        ) as e:
            raise Exception(f"Error While Running Query: {e}")

    def __get_args(
        self,
        region: str,
        database: str = None,
        workgroup: str = None,
        data_catalog: str = None,
        output_bucket: str = None,
        output_location: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ) -> dict[str, str]:
        """
        Get the arguments for the AthenaConnector class

        Args:
            `region (str)` : The AWS region
            `database (str)` : The database name
            `output_bucket (str)` : The output bucket name
            `output_location (str)` : The output location

        Returns:
            `dict[str, str]` : The arguments

        """

        def __check(k, v) -> None:
            if ("region" in v or k) and not isinstance(k, str):
                e = f"`{v}` -> Expected String Or Bytes-Like Object"
                raise TypeError(e)

        __check(region, "region")
        __check(database, "database")
        __check(workgroup, "workgroup")
        __check(data_catalog, "data_catalog")
        __check(output_bucket, "output_bucket")
        __check(output_location, "output_location")
        __check(aws_access_key_id, "aws_access_key_id")
        __check(aws_secret_access_key, "aws_secret_access_key")

        return {
            "region": region,
            "database": database,
            "workgroup": workgroup,
            "data_catalog": data_catalog,
            "output_bucket": output_bucket,
            "output_location": output_location,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }


"""
if __name__ == "__main__":

    # brief: This is the main function that is executed when the script is run.

    region = "us-west-1"
    database = "testing"
    workgroup = "primary"
    data_catalog = "AwsDataCatalog"
    output_bucket = "bhupenderhere"
    output_location = "athena_query_results/"
    aws_access_key_id = "862e5079-18b3-4f37-bdba-37724f21407b",
    aws_secret_access_key = "5647092e-033e-475f-95f7-b51f1875cda9"

    athena = AthenaConnector(
        region=region,
        database=database,
        workgroup=workgroup,
        data_catalog=data_catalog,
        output_bucket=output_bucket,
        output_location=output_location,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
"""
