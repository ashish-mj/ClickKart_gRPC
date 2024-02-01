import os
from concurrent import futures
import grpc
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions
from dotenv import load_dotenv
import protos.product_pb2_grpc as product_pb2_grpc
from protos import product_pb2
import google.protobuf.empty_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import ParseDict

class ProductServicer(product_pb2_grpc.productServiceServicer):

    def __init__(self):
        load_dotenv()
        self._cluster,self._connection = self.connectDB()

    def connectDB(self):
        conn_str = "couchbase://"+os.getenv("DB_HOST")
        try:
            cluster_opts = ClusterOptions(authenticator=PasswordAuthenticator(os.getenv("USERNAME"), os.getenv("PASSWORD")))
            cluster = Cluster(conn_str, cluster_opts)
        except CouchbaseException as error:
            print(f"Could not connect to cluster. Error: {error}")
            raise
        bucket = cluster.bucket(os.getenv("BUCKET"))
        collection = bucket.scope(os.getenv("SCOPE")).collection(os.getenv("COLLECTION"))
        return cluster,collection

    def createProduct(self, request, context):
        doc = MessageToDict(request)
        response = product_pb2.productId()
        try:
            self._connection.insert(request.productId, doc)
        except Exception as e:
            print(e)
            response.productId = "Exception Occured , Unable to Create Product"
            return response
        response.productId = request.productId
        return response

    def getProduct(self, request, context):
        response = product_pb2.product()
        try:
            result = self._connection.get(request.productId)
        except Exception as e:
            print(e)
            response.productId = "Exception Occured , Unable to Retrieve Product"
            return response
        doc = result.content_as[dict]
        response=ParseDict(doc,response)
        return response

    def updateProduct(self, request, context):
        doc = MessageToDict(request)
        print(doc)
        try:
            self._connection.replace(request.productId,doc)
        except Exception as e:
            print(e)
        return google.protobuf.empty_pb2.Empty()

    def deleteProduct(self, request, context):
        try:
            self._connection.remove(request.productId)
        except Exception as e:
            print(e)
        return google.protobuf.empty_pb2.Empty()

    def getAllProducts(self, request, context):
        query = "SELECT a.* FROM " + os.getenv("BUCKET") + " a"
        try:
            result = self._cluster.query(query)
        except Exception as e:
            print(e)
            response = product_pb2.product()
            response.productId = "Exception Occured , Unable to Retrieve All Products"
            return response
        for product in result.rows():
            response = product_pb2.product()
            response = ParseDict(product, response)
            yield response

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    product_pb2_grpc.add_productServiceServicer_to_server(ProductServicer(), server)
    server.add_insecure_port("localhost:5000")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    main()