from __future__ import annotations

from app.common.util import create_folder


class Repositories:
    DB = "_db"
    LOGS = "logs"
    REPORTS = "reports"
    TEMPORAL = "temp"

    EXCEL = f"{DB}/excel_files"
    S_REMOTO_EXCEL = f"{EXCEL}/s_remoto_excel"
    S_CENTRAL_EXCEL = f"{EXCEL}/s_central_excel"
    OUTPUT_MOTOR = f"{LOGS}/output"
    CONSIGNMENTS = f"{DB}/consignaciones"
    IMAGES = f"{REPORTS}/images"

    def __init__(self):
        self.repos = ["DB", "LOGS", "REPORTS", "TEMPORAL", "EXCEL", "S_REMOTO_EXCEL",
                      "S_CENTRAL_EXCEL", "OUTPUT_MOTOR", "CONSIGNMENTS", "IMAGES"]
        self.repo_path = {}
        for repo in self.repos:
            path = create_folder(getattr(self, repo))
            setattr(self, repo, path)
            self.repo_path[repo] = path

    def contains(self, repo_name: str) -> bool:
        return repo_name.upper() in self.repos

    def path_for(self, repo_name) -> str | None:
        return self.repo_path.get(repo_name.upper(), None)


local_repositories = Repositories()
