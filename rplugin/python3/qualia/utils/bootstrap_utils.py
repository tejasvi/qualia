from threading import Event

from qualia.config import GIT_BRANCH, GIT_TOKEN_URL, _GIT_FOLDER, _ROOT_ID_KEY
from qualia.models import Cursors, Client, CustomCalledProcessError
from qualia.utils.common_utils import cd_run_git_cmd, exception_traceback, StartLoggedThread, get_set_client
from qualia.utils.common_utils import get_time_uuid, logger, get_key_val, put_key_val, Database


def ensure_root_node(cursors: Cursors) -> None:
    if get_key_val(_ROOT_ID_KEY, cursors.metadata, False) is None:
        root_id = get_time_uuid()
        put_key_val(root_id, [''], cursors.content, False)
        put_key_val(root_id, [], cursors.children, False)
        put_key_val(root_id, [], cursors.parents, False)
        put_key_val(_ROOT_ID_KEY, root_id, cursors.metadata, False)


repository_setup = Event()


def setup_repository(client_data: Client) -> None:
    try:
        cd_run_git_cmd(["rev-parse", "--is-inside-work-tree"])
    except CustomCalledProcessError:
        cd_run_git_cmd(["init"])
        cd_run_git_cmd(["checkout", "-b", GIT_BRANCH])
        if GIT_TOKEN_URL:
            try:
                logger.debug("Fetching repository")
                cd_run_git_cmd(["fetch", GIT_TOKEN_URL, GIT_BRANCH])
                cd_run_git_cmd(["merge", "FETCH_HEAD"])
            except CustomCalledProcessError as e:
                logger.critical(f"Can't fetch and merge from {GIT_BRANCH}.\nError: " + exception_traceback(e))
                raise e
        gitattributes_path = _GIT_FOLDER.joinpath(".gitattributes")
        if not gitattributes_path.exists():
            with open(gitattributes_path, 'x') as f:
                f.write("*.md merge=union\n* text=auto eol=lf\n")
            cd_run_git_cmd(["add", "-A"])
            cd_run_git_cmd(["commit", "-m", "bootstrap"])
        cd_run_git_cmd(["config", "user.name", client_data["client_name"]])
        cd_run_git_cmd(["config", "user.email", f"{client_data['client_id']}@q.client"])
    repository_setup.set()


def bootstrap() -> None:
    with Database() as cursors:
        client_data = get_set_client(cursors.metadata)
        StartLoggedThread(target=lambda: setup_repository(client_data), name="SetupRepo")
        ensure_root_node(cursors)


