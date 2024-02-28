from locust import HttpUser, task, between
import json

class GraphQLUser(HttpUser):
    wait_time = between(1, 2)  # Adjust this to control wait time between requests for each user

    @task
    def graphql_query(self):
        query = """
            query gethuman() {
                human(id: "1000") {
                    name
                }
             }
        """
        headers = {"content-type": "application/json"}
        with self.client.post("/", json={"query": query}, headers=headers, catch_response=True) as response:
            # Check for successful response
            if response.status_code == 200:
                data = response.json()
            	# Validate response content if necessary
                expected_response = {"data":{"human":{"name":"Luke Skywalker"}}}  # Update this as per your expected response
                if data != expected_response:
                    response.failure("Received unexpected data: " + str(data))
                else:
                    response.success()
            else:
                response.failure("Failed with status code: " + str(response.status_code))
