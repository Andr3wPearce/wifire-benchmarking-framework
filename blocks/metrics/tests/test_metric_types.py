"""The types refuse anything the contract rules out, so metrics never have to check."""
import numpy as np
import pytest

from metric_types import Observation, ScoreRow
import metric_synthetic as syn

G = syn.grid((11, 11))


def obs(t, id_=None):
    return Observation(id=id_ or f"o{t}", time_min=t, burned=np.zeros(G.shape, bool), source="S")


def test_toa_must_be_2d():
    with pytest.raises(ValueError, match="2D"):
        syn.pair(np.zeros((2, *G.shape)), [obs(1.0)], G)


def test_shapes_must_match_the_grid():
    with pytest.raises(ValueError, match="shape"):
        syn.pair(np.zeros((5, 5)), [obs(1.0)], G)


def test_observations_sorted_unique_nonempty():
    toa = np.zeros(G.shape)
    with pytest.raises(ValueError, match="sorted"):
        syn.pair(toa, [obs(2.0), obs(1.0)], G)
    with pytest.raises(ValueError, match="unique"):
        syn.pair(toa, [obs(1.0, "a"), obs(2.0, "a")], G)
    with pytest.raises(ValueError, match="at least one"):
        syn.pair(toa, [], G, sim_end_min=1.0)


def test_score_row_status_rules():
    common = dict(metric="m", metric_version="1", unit="1", better="higher", scored_at_min=0.0,
                  observation_id="o", observation_source="S", prediction_id="p", reference_id="r")
    with pytest.raises(ValueError):
        ScoreRow(value=None, status="ok", reason=None, **common)          # ok needs a value
    with pytest.raises(ValueError):
        ScoreRow(value=0.5, status="undefined", reason="x", **common)     # not ok means no value
    with pytest.raises(ValueError):
        ScoreRow(value=None, status="unavailable", reason=None, **common)  # not ok needs a reason
