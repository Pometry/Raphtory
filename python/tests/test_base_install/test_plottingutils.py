import pytest
from raphtory import plottingutils


@pytest.mark.parametrize(
    "observations, cdf, ccdf",
    [
        ([1, 2, 3, 4], [1, 2, 3, 4], [4, 3, 2, 1]),
        ([1, 1, 2], [2, 2, 3], [3, 3, 1]),
        ([5], [1], [1]),
    ],
)
def test_cdf_and_ccdf_count_the_observations(observations, cdf, ccdf):
    n = len(observations)
    assert plottingutils.cdf(observations, normalised=False)[1].tolist() == cdf
    assert plottingutils.ccdf(observations, normalised=False)[1].tolist() == ccdf
    assert plottingutils.cdf(observations)[1].tolist() == [c / n for c in cdf]
    assert plottingutils.ccdf(observations)[1].tolist() == [c / n for c in ccdf]
