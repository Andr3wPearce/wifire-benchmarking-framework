"""Tests for overlap.iou: the pattern every metric's tests follow (docs/metrics_contract.md,
"Testing"). Synthetic circular fires give answers you can check by hand."""
import numpy as np
import pytest

from overlap import iou
import metric_synthetic as syn

G = syn.grid((101, 101), res_m=30.0)
ROS = 20.0                     # m/min
T = 60.0                       # observed at 60 min: a disc of radius 1200 m (40 cells)


def only(rows):
    assert len(rows) == 1
    return rows[0]


def test_perfect_prediction_scores_1():
    p = syn.pair(syn.circular_toa(G, ROS), [syn.circular_observation(G, ROS, T)], G)
    row = only(iou(p))
    assert row.status == "ok"
    assert row.value == 1.0
    assert row.better == "higher" and row.unit == "1"


def test_disjoint_prediction_scores_0():
    # prediction ignites in a far corner and stays small; observation is in the centre
    toa = syn.circular_toa(G, ROS / 4, center_rc=(5, 5))
    row = only(iou(syn.pair(toa, [syn.circular_observation(G, ROS, 20.0)], G)))
    assert row.status == "ok" and row.value == 0.0


def test_slow_prediction_matches_concentric_disc_formula():
    # concentric discs: IoU = (r_pred / r_obs)^2 = (ROS_pred / ROS_obs)^2 = 0.25
    toa = syn.circular_toa(G, ROS / 2)
    row = only(iou(syn.pair(toa, [syn.circular_observation(G, ROS, T)], G)))
    assert row.value == pytest.approx(0.25, abs=0.02)        # discretization
    d = row.diagnostics
    assert d["intersection_cells"] == d["predicted_cells"]   # the small disc is inside the big one
    assert row.value == d["intersection_cells"] / d["union_cells"]


def test_nothing_burned_anywhere_is_undefined():
    toa = np.full(G.shape, np.nan)
    obs = syn.Observation(id="empty", time_min=T, burned=np.zeros(G.shape, bool), source="SYNTHETIC")
    row = only(iou(syn.pair(toa, [obs], G)))
    assert row.status == "undefined" and row.value is None and row.reason


def test_observation_after_simulation_end_is_unavailable():
    p = syn.pair(syn.circular_toa(G, ROS, sim_end_min=30.0), [syn.circular_observation(G, ROS, T)], G,
                 sim_end_min=30.0)
    row = only(iou(p))
    assert row.status == "unavailable" and row.value is None
    assert "30" in row.reason and "60" in row.reason


def test_invalid_cells_do_not_count():
    # prediction overburns into the eastern half; marking that half invalid removes the error
    toa = syn.circular_toa(G, ROS)
    obs = syn.circular_observation(G, ROS, T)
    overburn = toa.copy()
    overburn[:, 51:] = 0.0                                  # everything east of centre "burned"
    row = only(iou(syn.pair(overburn, [obs], G)))
    assert row.value < 1.0                                  # the overburn counts...
    assert row.value == row.diagnostics["intersection_cells"] / row.diagnostics["union_cells"]
    valid = np.ones(G.shape, bool)
    valid[:, 51:] = False
    assert only(iou(syn.pair(overburn, [obs], G, valid=valid))).value == 1.0


def test_one_row_per_observation_in_order():
    toa = syn.circular_toa(G, ROS / 2)
    obs = [syn.circular_observation(G, ROS, t) for t in (20.0, 40.0, 60.0)]
    rows = iou(syn.pair(toa, obs, G))
    assert [r.observation_id for r in rows] == [o.id for o in obs]
    assert [r.scored_at_min for r in rows] == [20.0, 40.0, 60.0]


def test_rows_carry_lineage_and_flags():
    p = syn.pair(syn.circular_toa(G, ROS), [syn.circular_observation(G, ROS, T)], G,
                 flags={"hit_domain_edge"})
    row = only(iou(p))
    assert row.prediction_id == "test-prediction" and row.reference_id == "test-reference"
    assert row.flags == ("hit_domain_edge",)
    assert row.to_dict()["flags"] == ["hit_domain_edge"]
