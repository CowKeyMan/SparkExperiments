from utils import get_pivot


def test_get_pivot():
    assert get_pivot(1250, 100, 500, [(1, 2, 3, 4), (300, 250, 100)]) \
        == (2, "right")
    assert get_pivot(1250, 100, 500, [(1, 2, 3, 4), (3, 2.5, 1)]) \
        == (2, "right")
    assert get_pivot(1000, 50, 50, [(1, 2, 3, 4), (300, 300, 300)]) \
        == (3, "left")
    assert get_pivot(1000, 100, 300, [(1, 2, 3, 4), (400, 100, 100)]) \
        == (2, "right")
    assert get_pivot(1000, 300, 100, [(1, 2, 3, 4), (200, 200, 100)]) \
        == (2, "left")
    assert get_pivot(
        1000, 300, 100, [(1, 2, 3, 4, 5, 6), (100, 50, 50, 200, 200)]
    ) == (4, "left")
