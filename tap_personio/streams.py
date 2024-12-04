"""Stream type classes for tap-personio."""

from __future__ import annotations
from array import array
import re

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_personio.client import PersonioStream

class EmployeeAttributesStream(PersonioStream):
    name = "employee_attributes"
    path = "/company/employees/attributes"
    primary_keys = ["id"]
    replication_key = None

    records_jsonpath = "$.data[*]"

    @property
    def schema(self):
        return th.PropertiesList().to_dict()

class EmployeesStream(PersonioStream):
    name = "employee"
    path = "/company/employees"
    primary_keys = ["id"]
    records_jsonpath = "$.data[*].attributes"
    replication_key = None

    discovered_schema = None

    @property
    def schema(self):
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """

        if self.discovered_schema != None:
            return self.discovered_schema
   
        properties: List[th.Property] = []
        
        # id is mandatory and always present, the rest depends on the configuration of the API token
        properties.append(
            th.Property(
                "id",
                th.IntegerType,
                description="The user's system ID",
            )
        )

        # hack: we have to discover the schema dynmically based on the data that's coming off personio
        # however, we can't just make an http request from this class instance since .schema is called from the 
        # ctor (bad! never call a virtual function from a ctor...) and thus all the http stuff is not initialized yet.
        # We therefore create a separate instance of our base class that has a purposefully empty schema to make that 
        # request, then build our schema off of that.
        discovery = EmployeeAttributesStream(self._tap, None, None)
    
        records = [x for x in discovery.request_records(None)]

        for attr in discovery.request_records(None):
            personio_type = attr["type"]
            personio_id = attr["universal_id"] or attr["label"].lower()
            
            # see https://developer.personio.de/reference/get_company-employees-attributes
            json_type = th.StringType()
            if (personio_type == "integer"):
                json_type = th.IntegerType()
            elif personio_type == "decimal":
                json_type = th.NumberType()
            elif personio_type == "tags":
                json_type = th.ArrayType(th.StringType)
            elif personio_type == "date":
                json_type = th.DateTimeType()
            elif personio_id == "department":
                json_type = th.ObjectType(
                    th.Property("id", th.IntegerType()), 
                    th.Property("name", th.StringType())
                )
            elif personio_id == "team":
                json_type = th.ObjectType(
                    th.Property("id", th.IntegerType()), 
                    th.Property("name", th.StringType())
                )
            elif personio_id == "cost_centers":
                json_type = th.ArrayType(
                    th.ObjectType(
                        th.Property("id", th.IntegerType()),
                        th.Property("name", th.StringType()),
                        th.Property("percentage", th.NumberType())
                    )
                )
            elif personio_id == "supervisor":
                json_type = th.ObjectType(
                    th.Property("id", th.ObjectType(th.Property("value", th.IntegerType()))),
                    th.Property("first_name", th.ObjectType(th.Property("value", th.StringType()))),
                    th.Property("last_name", th.ObjectType(th.Property("value", th.StringType()))),
                    th.Property("preferred_name", th.ObjectType(th.Property("value", th.StringType()))),
                )
            elif personio_id == "subcompany":
                json_type = th.ObjectType(
                    th.Property("id", th.IntegerType()),
                    th.Property("name", th.StringType())
                )
            elif personio_id == "office":
                json_type = th.ObjectType(
                    th.Property("id", th.IntegerType()),
                    th.Property("name", th.StringType())
                )
            # note: the PersonioAPI is a mess, we probably need more here

            properties.append(th.Property(personio_id, json_type, description=attr["label"]))            
            
        self.discovered_schema = th.PropertiesList(*properties).to_dict()

        return self.discovered_schema

    def post_process(self, row: dict, context: dict | None = None) -> dict | None: 
        result = dict()

        # flatten the "meta" attributes into something more closely resembling a record
        for key in row:
            attr = row[key]

            value = attr["value"]
            is_object_type = type(value) is dict and "attributes" in value
            is_array_type = type(value) is list
            
            personio_id = attr["universal_id"] or attr["label"].lower()
            if is_object_type:
                result[personio_id] = value["attributes"]
            elif is_array_type:
                result[personio_id] = [x["attributes"] for x in value]
            else:
                result[personio_id] = value

        return result