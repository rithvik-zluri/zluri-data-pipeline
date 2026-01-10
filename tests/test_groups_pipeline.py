def test_group_inactive_logic():
    """
    If group has no active users and no child groups with active users → inactive
    """
    from src.pipelines.groups_pipeline import is_group_active

    group_members = []
    child_group_active = False

    status = is_group_active(group_members, child_group_active)

    assert status == False
