#!/usr/bin/env python
# coding: utf-8

from neo4j import GraphDatabase, basic_auth
import boto3, csv, requests
import pandas as pd
from io import StringIO
from concurrent.futures import ThreadPoolExecutor

# Connection to Neo4j
class Neo4jConnection:
    """
    Represents a connection to a Neo4j database.

    Args:
        uri (str): The URI of the Neo4j database.
        user (str): The username for authentication.
        pwd (str): The password for authentication.

    Attributes:
        __uri (str): The URI of the Neo4j database.
        __user (str): The username for authentication.
        __password (str): The password for authentication.
        __driver (neo4j.Driver): The Neo4j driver object.

    Methods:
        close(): Closes the connection to the Neo4j database.
        get_assumed_session(role_arn, role_session_name, region_name): Retrieves an assumed session using AWS STS to assume a role.
        generate_presigned_url(session, bucket_name, object_key, expiration=3600): Generates a presigned URL for accessing an object in an S3 bucket.
        query(query, parameters=None, db=None): Executes a Cypher query on the Neo4j database.
        get_node_count(db=None): Retrieves the total number of nodes in the graph database.
        get_relationship_count(db=None): Retrieves the count of relationships in the graph database.
        delete_all(db=None): Deletes all nodes and relationships in the graph database.
        rename_label(old_label, new_label, db=None): Renames a label in the graph database.
        upload_node(file_url, file_type, node_label, id_property, compression=None, batch_size=10000, create_merge="CREATE"): Uploads a file to create nodes in the Neo4j database using batch processing.
        upload_relationships_schema(file_url, delimiter=',', headers=True, db='neo4j'): Uploads a file to create relationships in the Neo4j database using batch processing.
        relationship_by_property(from_node_label, to_node_label, from_property_name, to_property_name, rel_type, rel_props, batch_size=10000, create_merge="MERGE"): Creates a relationship between two nodes in the Neo4j database based on a common property.
        relationship_by_inproperty(from_node_label, to_node_label, from_property_name, to_property_list_name, rel_type, rel_props, batch_size=10000, create_merge="MERGE"): Creates a relationship between two nodes in the Neo4j database based on a common property.
        relationships_in_batches(file_url, file_type, node1_label, node2_label, node1_id, node2_id, id1_column, id2_column, relationship_type, batch_size=10000, compression=None, create_merge="MERGE"): Processes relationships between two nodes in Neo4j in batches from a CSV, TSV, or JSON file using apoc.periodic.iterate.
        show_databases(): Retrieves a list of all databases in the Neo4j instance.
        delete_test_data(): Deletes all nodes with a 'test' property from the Neo4j database.
        delete_all_data(): Deletes all nodes and relationships from the Neo4j database.
        inspect_schema(): Retrieves the schema visualization of the Neo4j database.
        get_properties(entity_type, entity_label): Retrieves the properties of nodes, relationships, or constraints in the Neo4j database.

    Example usage:
        conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
        conn.query("MATCH (n) RETURN n LIMIT 10")
        conn.close()
    """

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__password = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=basic_auth(self.__user, self.__password))
        except Exception as e:
            print(f"Failed to create the driver: {e}")

    def close(self):
        """
        Closes the connection to the Neo4j database.
        """
        if self.__driver is not None:
            self.__driver.close()
 
    def get_assumed_session(self, role_arn, role_session_name, region_name, access_key_id, secret_access_key):
        """
        Retrieves an assumed session using AWS STS to assume a role.

        Args:
            role_arn (str): The ARN of the role to assume.
            role_session_name (str): The name of the role session.
            region_name (str): The AWS region name.
            access_key_id (str): The AWS access key ID.
            secret_access_key (str): The AWS secret access key.

        Returns:
            boto3.Session: The assumed session.

        Example usage:
            session = get_assumed_session("arn:aws:iam::123456789012:role/MyRole", "MySession", "us-west-2", "ACCESS_KEY", "SECRET_KEY")
        """
        try:
            sts_client = boto3.client('sts', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
            assumed_role_object = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=role_session_name
            )
            
            credentials = assumed_role_object['Credentials']
            
            assumed_session = boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=region_name
            )
            print("Assumed role successfully.")
            return assumed_session
        
        except Exception as e:
            print(f"Failed to assume role: {e}")
            return None

    def generate_presigned_url(self, session, bucket_name, object_key, expiration=3600):
        """
        Generates a presigned URL for accessing an object in an S3 bucket.

        Args:
        session (boto3.Session): The AWS session object.
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the object in the S3 bucket.
        expiration (int, optional): The expiration time of the presigned URL in seconds. Defaults to 3600.

        Returns:
        str: The presigned URL.

        Example usage:
        session = get_assumed_session("arn:aws:iam::123456789012:role/MyRole", "MySession", "us-west-2")
        url = generate_presigned_url(session, "my-bucket", "my-object.txt")
        """
        s3_client = session.client('s3')
        url = s3_client.generate_presigned_url('get_object',
                                               Params={'Bucket': bucket_name, 'Key': object_key},
                                               ExpiresIn=expiration)
        print("Presigned URL generated successfully.")
        return url

    def query(self, query, parameters=None, db=None):
        """
        Executes a Cypher query on the Neo4j database.

        Args:
            query (str): The Cypher query to execute.
            parameters (dict, optional): The parameters to pass to the query. Defaults to None.
            db (str, optional): The name of the database to execute the query on. Defaults to None.

        Returns:
            list: The result of the query as a list of records.

        Raises:
            AssertionError: If the driver is not initialized.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            result = conn.query("MATCH (n) RETURN n LIMIT 10")
            conn.close()
        """
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

    def get_node_count(self, db=None):
        """
        Retrieves the total number of nodes in the graph database.

        Parameters:
        - db (str): Optional parameter specifying the database to query. If not provided, the default database will be used.

        Returns:
        - int: The total number of nodes in the graph database.

        Example:
        ```
        db_connection = DBConnection()
        node_count = db_connection.get_node_count()
        print(node_count)
        100
        ```
        """
        query = "MATCH (n) RETURN count(n) AS numberOfNodes"
        result = self.query(query, db=db)
        for record in result:
            return record["numberOfNodes"]

    def get_relationship_count(self, db=None):
        """
        Retrieves the count of relationships in the graph database.
        Parameters:
        - db (optional): The name of the database to query. If not provided, the default database will be used.
        Returns:
        - numberOfRelationships (int): The count of relationships in the graph database.
        Raises:
        - None

        Example usage:
        ```
        connection = DBConnection()
        count = connection.get_relationship_count()
        print(count)  # Output: 10
        ```
        """
        
        query = "MATCH ()-[r]->() RETURN count(r) AS numberOfRelationships"
        result = self.query(query, db=db)
        for record in result:
            return record["numberOfRelationships"]

    def delete_all(self, db=None):
        """
        Deletes all nodes and relationships in the graph database.
        Parameters:
        - db (optional): The name of the database to query. If not provided, the default database will be used.
        Returns:
        - None
        Example usage:
        ```
        connection = DBConnection()
        connection.delete_all()
        ```
        """
        query = "MATCH (n) DETACH DELETE n"
        self.query(query, db=db)
        print("All nodes and relationships have been deleted.")

    def rename_label(self, old_label, new_label, db=None):
        """
        Renames a label in the graph database.
        Parameters:
        - old_label (str): The old label to rename.
        - new_label (str): The new label to rename to.
        - db (optional): The name of the database to query. If not provided, the default database will be used.
        Returns:
        - None
        Example usage:
        ```
        connection = DBConnection()
        connection.rename_label("Person", "User")
        ```
        """

        query = f"CALL apoc.refactor.rename.label('{old_label}', '{new_label}')"
        self.query(query, db=db)
        print(f"Label '{old_label}' has been renamed to '{new_label}'.")

    # Methods for creating nodes in the Neo4j database

    def upload_node(self, file_url, file_type, node_label, id_property, compression=None, batch_size=10000, create_merge="CREATE"):
        """
        Uploads a file to create nodes in the Neo4j database using batch processing.

        Args:
            file_url (str): The path to the file.
            file_type (str): The type of the file ('csv', 'tsv', 'json').
            node_label (str): The label to assign to the created nodes.
            id_property (str): The property to use as the node identifier.
            compression (str, optional): The compression type if the file is compressed ('gzip'). Defaults to None.
            sep (str, optional): The separator used in the file. Defaults to ',' for CSV.
            batch_size (int, optional): The number of records to process in each batch. Defaults to 1000.
            create_merge (str, optional): The operation to perform ('CREATE', 'MERGE'). Defaults to 'CREATE'.

        Returns:
            list: The result of the query as a list of records.
        """
        if file_type == 'json' and compression:
            raise ValueError("Compression is not supported for JSON files with apoc.load.json.")
        
        # Define the separator based on the file type
        try:
            seps = {
                'csv': "','",
                'tsv': "'\\t'",
                'json': None
            }
            sep = seps.get(file_type)
        except Exception as e:
            print(f"An error occurred when selecting the separator using the file type: {e}")

        # Adjust the query based on whether compression is used
        if compression:
            query_template = f"""
            CALL apoc.periodic.iterate(
            "CALL apoc.load.csv('{file_url}', {{sep: {sep}, compresion: '{compression}'}}) YIELD map AS row",
            "{create_merge} (n:{node_label} {{id: row.{id_property}}}) SET n += row",
            {{batchSize:{batch_size}, iterateList:true, parallel:true}}
            )
            """
        else:
            query_template = f"""
            CALL apoc.periodic.iterate(
            "CALL apoc.load.csv('{file_url}', {{sep: {sep}}}) YIELD map AS row",
            "{create_merge} (n:{node_label} {{id: row.{id_property}}}) SET n += row",
            {{batchSize:{batch_size}, iterateList:true, parallel:true}}
            )
            """
        
        queries = {
            'csv': query_template,
            'tsv': query_template,  # Properly escaped the tab character using the template
            'json': f"""
            CALL apoc.periodic.iterate(
            "CALL apoc.load.json('{file_url}') YIELD value AS row",
            "{create_merge} (n:{node_label} {{id: row.{id_property}}}) SET n += row",
            {{batchSize: {batch_size}, iterateList: true, parallel: true}}
            )
            """
        }
        query = queries.get(file_type)
        if query is None:
            raise ValueError("Unsupported file type. Supported types are: 'csv', 'tsv', 'json'.")
        return self.query(query)

    # Methods for creating relationships between nodes in the Neo4j database

    def upload_relationships_schema(self, file_url, delimiter=',', headers=True, db='neo4j'):
        try:
            response = requests.get(file_url)
            response.raise_for_status()
            csv_data = StringIO(response.text)
            reader = csv.DictReader(csv_data, delimiter=delimiter)

            for row in reader:
                currentRelType = row['currentRelType'].strip('"""')
                from_node = row['from_node'].strip('"""')
                from_node_property = row['from_node_property'].strip('"""') if row['from_node_property'] else None
                to_node_property = row['to_node_property'].strip('"""') if row['to_node_property'] else None
                to_node = row['to_node'].strip('"""')

                if from_node_property and to_node_property:
                    print(f"Creating relationships of type '{currentRelType}' between '{from_node}' and '{to_node}' nodes. Using properties '{from_node_property}' and '{to_node_property}'.")
                    
                    query = f"""
                        MATCH (m:{from_node})
                        OPTIONAL MATCH (b:{to_node} {{{to_node_property}: m.{from_node_property}}})
                        WITH m, b
                        WHERE b IS NOT NULL
                        MERGE (m)-[r:{currentRelType}]->(b)
                        SET r.{from_node_property} = m.{from_node_property}, r.{to_node_property} = b.{to_node_property}
                        RETURN count(*)
                    """

                response = self.query(query, db=db)
                if response:
                    print(f"Relationships created as {response}")

            print("Relationships from schema uploaded successfully.")
            return True

        except requests.exceptions.RequestException as e:
            print(f"Failed to download relationships schema from {file_url}: {e}")
            return False
    
    def relationship_by_property(self, from_node_label: str, to_node_label: str, from_property_name: str, to_property_name: str, rel_type: str, rel_props: dict, batch_size = 10000, create_merge="MERGE"):
        """
        Creates a relationship between two nodes in the Neo4j database based on a common property.

        Args:
            tx (Transaction): The Neo4j transaction object.
            from_node_label (str): The label of the source node.
            to_node_label (str): The label of the target node.
            from_property_name (str): The name of the property in the source node.
            to_property_name (str): The name of the property in the target node.
            rel_type (str): The type of the relationship.
            rel_props (dict): A dictionary containing the relationship properties.

        Returns:
            None
        """
        try:
            self.query(
                f"""
                CALL apoc.periodic.iterate(
                "MATCH (a:{from_node_label}), (b:{to_node_label})
                WHERE a.{from_property_name} = b.{to_property_name}
                RETURN a, b",
                "{create_merge} (a)-[r:{rel_type}]->(b)
                SET r += {rel_props}
                RETURN count(*)",
                {{batchSize:{batch_size}, iterateList: true, parallel:true}})
                """
            )
            print("Query submitted.")

        except Exception as e:
            print("Execution had an error: ", e)

    def relationship_by_inproperty(self, from_node_label: str, to_node_label: str, from_property_name: str, to_property_list_name: str, rel_type: str, rel_props: dict, batch_size = 10000, create_merge="MERGE"):
        """
        Creates a relationship between two nodes in the Neo4j database based on a common property.

        Args:
            from_node_label (str): The label of the source node.
            to_node_label (str): The label of the target node.
            from_property_name (str): The name of the property in the source node.
            to_property_name (str): The name of the property in the target node.
            rel_type (str): The type of the relationship.
            rel_props (dict): A dictionary containing the relationship properties.
            batch_size (int, optional): The number of records to process in each batch. Defaults to 10000.
            create_merge (str, optional): The operation to perform ('CREATE', 'MERGE'). Defaults to 'MERGE'.

        Returns:
            None
        """
        try:
            query_template = f"""
            CALL apoc.periodic.iterate(
            "MATCH (a:{from_node_label}), (b:{to_node_label})
            WHERE ANY(item IN b.{to_property_list_name} WHERE item CONTAINS a.{from_property_name})
            RETURN a, b",
            "{create_merge} (a)-[r:{rel_type}]->(b)
            SET r += {rel_props}",
            {{batchSize:{batch_size}, iterateList: true, parallel:true}})
            """
            self.query(query_template)
            print("Query submitted.")
        except Exception as e:
            print("Execution had an error: ", e)

    def relationships_in_batches(self, file_url, file_type, node1_label, node2_label, node1_id, node2_id, id1_column, id2_column, relationship_type, batch_size=10000, compression=None, create_merge="MERGE"):
        """
        Processes relationships between two nodes in Neo4j in batches from a CSV, TSV, or JSON file using apoc.periodic.iterate.

        Args:
            file_url (str): The URL to the file containing the relationships.
            file_type (str): The type of the file ('csv', 'tsv', 'json').
            node1_label (str): Label for the first node.
            node2_label (str): Label for the second node.
            node1_id (str): Property name for the first node's ID.
            node2_id (str): Property name for the second node's ID.
            id1_column (str): The header name of the first node's ID in the file.
            id2_column (str): The header name of the second node's ID in the file.
            relationship_type (str): The type of relationship to create.
            batch_size (int, optional): The number of rows to process in each batch. Defaults to 1000.
            compression (str, optional): The compression type if the file is compressed ('gzip'). Defaults to 'gzip'.
            db (str, optional): The database to run the query against. Defaults to None.
            create_merge (str, optional): Use "CREATE" or "MERGE" for relationship creation. Defaults to "MERGE".
        """
        if file_type == 'json' and compression:
            raise ValueError("Compression is not supported for JSON files with apoc.load.json.")

        # Define the separator based on the file type
        # DO NOT CHANGE the way the string is built here,
        # as it is used in the query template and the tab space can't be escaped in the query
        seps = {
                'csv': "','",
                'tsv': "'\\t'",
                'json': 'No separator'
        }
        sep = seps.get(file_type)
        if sep is None:
            raise ValueError("Unsupported file type. Supported types are: 'csv', 'tsv', 'json'.")

        # Adjust the query based on whether compression is used
        if compression:
            query_template = f"""
                CALL apoc.periodic.iterate(
                "CALL apoc.load.csv('{file_url}', {{sep: {sep}, compression: '{compression}'}}) YIELD map AS row",
                "MATCH (a:{node1_label} {{{node1_id}: row['{id1_column}']}}), 
                    (b:{node2_label} {{{node2_id}: row['{id2_column}']}})
                {create_merge} (a)-[r:{relationship_type}]->(b)
                SET r += row",
                {{batchSize: {batch_size}, iterateList: true, parallel: true}}
                )
            """

        else:
            query_template = f"""
                CALL apoc.periodic.iterate(
                "CALL apoc.load.csv('{file_url}', {{sep: {sep}}}) YIELD map AS row",
                "MATCH (a:{node1_label} {{{node1_id}: row['{id1_column}']}}), 
                    (b:{node2_label} {{{node2_id}: row['{id2_column}']}})
                {create_merge} (a)-[r:{relationship_type}]->(b)
                SET r += row",
                {{batchSize: {batch_size}, iterateList: true, parallel: true}}
                )
            """

        # Define the templates based on file types
        templates = {
            'csv': query_template,
            'tsv': query_template,
            'json': f"""
                CALL apoc.periodic.iterate(
                "CALL apoc.load.json('{file_url}') YIELD value AS row",
                "MATCH (a:{node1_label} {{{node1_id}: row['{id1_column}']}}), 
                    (b:{node2_label} {{{node2_id}: row['{id2_column}']}})
                {create_merge} (a)-[r:{relationship_type}]->(b)
                SET r += row",
                {{batchSize: {batch_size}, iterateList: true, parallel: true}}
                )
            """
        }

        # Select the query template based on the file type
        query_template = templates.get(file_type)
        if query_template is None:
            raise ValueError("Unsupported file type. Supported types are: 'csv', 'tsv', 'json'.")

        parameters = {
            'batch_size': batch_size
        }

        self.query(query_template, parameters=parameters)
        print("Query submitted.")

    # Additional methods in progress - not yet fully implemented 

    def show_databases(self):
        """
        Retrieves a list of all databases in the Neo4j instance.

        Returns:
            list: The list of databases.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            databases = conn.show_databases()
            conn.close()
        """
        return self.query("SHOW DATABASES")

    def inspect_schema(self):
        """
        Retrieves the schema visualization of the Neo4j database.

        Returns:
            list: The schema visualization.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            schema = conn.inspect_schema()
            conn.close()
        """
        return self.query("CALL db.schema.visualization()")

    def get_properties(self, entity_type, entity_label):
        """
        Retrieves the properties of nodes, relationships, or constraints in the Neo4j database.

        Args:
            entity_type (str): The type of entity to retrieve properties for. Must be 'node', 'relationship', or 'constraint'.
            entity_label (str): The label of the entity to retrieve properties for.

        Raises:
            ValueError: If the entity_type is invalid.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            conn.get_properties('node', 'Person')
            conn.close()
        """
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                if entity_type == 'node':
                    result = session.execute_read(self._get_all_node_properties, entity_label)
                elif entity_type == 'relationship':
                    result = session.execute_read(self._get_all_relationship_properties, entity_label)
                elif entity_type == 'constraint':
                    result = session.execute_read(self._get_all_constraints, entity_label)
                else:
                    raise ValueError(f"Invalid entity_type: {entity_type}. Must be 'node', 'relationship', or 'constraint'.")

                for record in result:
                    if record:
                        print(record)
                    else:
                        print("No properties found for the given relationship.")
        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def _get_all_node_properties(tx, node_label):
        query = f"""
        MATCH (n:{node_label})
        RETURN properties(n) AS properties
        """
        print(f"Executing query: {query}")  # Debugging print
        result = tx.run(query).data()
        print(f"Query result: {result}")  # Debugging print
        return result

    @staticmethod
    def _get_all_relationship_properties(tx, relationship_type):
        query = f"""
        MATCH ()-[r:{relationship_type}]->()
        RETURN type(r) AS type, properties(r) AS properties
        """
        print(f"Executing query: {query}")  # Debugging print
        result = tx.run(query).data()
        print(f"Query result: {result}")  # Debugging print
        return result

    @staticmethod
    def _get_all_constraints(tx, label):
        query = f"""
        SHOW CONSTRAINTS FOR (n:{label})
        """
        print(f"Executing query: {query}")  # Debugging print
        result = tx.run(query).data()
        print(f"Query result: {result}")  # Debugging print
        return result
