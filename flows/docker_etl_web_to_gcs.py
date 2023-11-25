from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_web_to_gcs import etl_parent_web_to_gcs


docker_block = DockerContainer.load("etl-web-to-gcs-docker")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_web_to_gcs,
    name="docker-etl-web-to-gcs",
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()