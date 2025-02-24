from flask import Flask, jsonify

app = Flask(__name__)


MOCK_ENDPOINTS = {
    "/api/internal/domains/default/sql/schema/catalogs/spark-configs": {
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hadoop",
        "spark.sql.catalog.spark_catalog.warehouse": "tmp/lakehouse",
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.type": "hadoop",
        "spark.sql.catalog.iceberg_catalog.warehouse": "tmp/ice_catalog"
    },
    "/api/internal/domains/default/sql/schema/catalogs": [
        "spark_catalog",
        "iceberg_catalog",
    ],
    "/api/internal/domains/default/spark/settings/catalog-names": [
        "iceberg_catalog",
    ],
    "/api/v1/authz/lakehouses/test": {}
}

@app.route('/<path:endpoint>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def handle_request(endpoint):
    # Remove leading/trailing slashes and convert to lowercase
    normalized_endpoint = "/" + endpoint.strip('/').lower()

    # Check if endpoint exists in our mock data
    if normalized_endpoint in MOCK_ENDPOINTS:
        response_data = MOCK_ENDPOINTS[normalized_endpoint]
        return jsonify(response_data), 200

    return jsonify({"error": "Endpoint not found"}), 404

if __name__ == '__main__':
    app.run(debug=True, port=3000)
