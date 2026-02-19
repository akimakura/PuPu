from src.integration.worker_manager.client import ClientWorkerManager as ClientWorkerManager


def get_worker_manager_client() -> ClientWorkerManager:
    return ClientWorkerManager()
