from flashburst.capabilities.loader import load_capability
from flashburst.config import add_capability_import
from flashburst.capabilities.registry import all_capabilities


def test_load_capability_from_project_import_path(external_workload_project) -> None:
    capability = load_capability(
        external_workload_project.capability_import,
        project_root=external_workload_project.root,
    )

    assert capability.spec.name == external_workload_project.capability_name
    assert capability.local_runner is not None


def test_workspace_registered_capability_extends_registry(
    tmp_path,
    external_workload_project,
) -> None:
    workspace = tmp_path / ".flashburst"
    add_capability_import(
        workspace=workspace,
        import_path=external_workload_project.capability_import,
        project_root=str(external_workload_project.root),
    )

    capabilities = all_capabilities(workspace=workspace)

    assert "embedding.fake-deterministic" in capabilities
    assert external_workload_project.capability_name in capabilities
