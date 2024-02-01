import protos.product_pb2 as product_pb2
import protos.product_pb2_grpc as product_pb2_grpc
import google.protobuf.empty_pb2
import grpc

stub=None

def createProduct():
    request = input("Enter the field values in the mentioned sequence - Product Id,Product Name, Price, Tax, Status, Description\n ").split()[:6]
    request = product_pb2.product(productId=request[0], productName=request[1], price=float(request[2]), tax=float(request[3]),status=request[4], description=request[5])
    response = stub.createProduct(request)
    print(response)

def getProduct():
    request = input("Enter the Product Id to be retrieved\n")
    request = product_pb2.productId(productId=request)
    response = stub.getProduct(request)
    print(response)

def updateProduct():
    request = input("Enter the field values in the mentioned sequence - Product Id,Product Name, Price, Tax, Status, Description\n ").split()[:6]
    request = product_pb2.product(productId=request[0], productName=request[1], price=float(request[2]),tax=float(request[3]),status=request[4], description=request[5])
    response = stub.updateProduct(request)
    print(response)

def deleteProduct():
    request = input("Enter the Product Id to be retrieved\n")
    request = product_pb2.productId(productId=request)
    response = stub.deleteProduct(request)
    print(response)

def getAllProducts():
    request = google.protobuf.empty_pb2.Empty()
    response = stub.getAllProducts(request)
    for product in response:
        print(product)

def main():
    with grpc.insecure_channel('localhost:5000') as channel:
        global stub
        iter=True
        stub = product_pb2_grpc.productServiceStub(channel)

        while iter:
            choice = int(input("Enter the Choice\n 1. Create Product\n 2. Get Product\n 3. Update Product\n 4. Delete product\n 5. Get All products\n"))
            if choice==1:
                createProduct()
            elif choice==2:
                getProduct()
            elif choice==3:
                updateProduct()
            elif choice==4:
                deleteProduct()
            elif choice==5:
                getAllProducts()
            else:
                iter=False

if __name__ == "__main__":
    main()