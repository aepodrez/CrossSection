"""
PyDataDownloads package
Contains Python equivalents of Stata .do files from DataDownloads directory.

Generated files: 39
"""

# Import all download functions
from .a_ccmlinkingtable import a_ccmlinkingtable
from .b_compustatannual import b_compustatannual
from .c_compustatquarterly import c_compustatquarterly
from .d_compustatpensions import d_compustatpensions
from .e_compustatbusinesssegments import e_compustatbusinesssegments
from .f_compustatcustomersegments import f_compustatcustomersegments
from .g_compustatshortinterest import g_compustatshortinterest
from .h_crspdistributions import h_crspdistributions
from .i2_crspmonthlyraw import i2_crspmonthlyraw
from .i_crspmonthly import i_crspmonthly
from .j_crspdaily import j_crspdaily
from .k_crspacquisitions import k_crspacquisitions
from .l2_ibes_eps_adj import l2_ibes_eps_adj
from .l_ibes_eps_unadj import l_ibes_eps_unadj
from .m_ibes_recommendations import m_ibes_recommendations
from .n_ibes_unadjustedactuals import n_ibes_unadjustedactuals
from .o_daily_fama_french import o_daily_fama_french
from .p_monthly_fama_french import p_monthly_fama_french
from .q_marketreturns import q_marketreturns
from .r_monthlyliquidityfactor import r_monthlyliquidityfactor
from .s_qfactormodel import s_qfactormodel
from .t_vix import t_vix
from .u_gnpdeflator import u_gnpdeflator
from .v_tbill3m import v_tbill3m
from .w_brokerdealerleverage import w_brokerdealerleverage
from .x2_ciqcreditratings import x2_ciqcreditratings
from .x_spcreditratings import x_spcreditratings
from .za_ipodates import za_ipodates
from .zb_pin import zb_pin
from .zc_governanceindex import zc_governanceindex
from .zd_corwinschultz import zd_corwinschultz
from .ze_13f import ze_13f
from .zf_crspibeslink import zf_crspibeslink
from .zg_bidasktaq import zg_bidasktaq
from .zh_optionmetrics import zh_optionmetrics
from .zi_patentcitations import zi_patentcitations
from .zj_inputoutputmomentum import zj_inputoutputmomentum
from .zk_customermomentum import zk_customermomentum
from .zl_crspoptionmetrics import zl_crspoptionmetrics


# List of all available download functions
DOWNLOAD_FUNCTIONS = [
    a_ccmlinkingtable,
    b_compustatannual,
    c_compustatquarterly,
    d_compustatpensions,
    e_compustatbusinesssegments,
    f_compustatcustomersegments,
    g_compustatshortinterest,
    h_crspdistributions,
    i2_crspmonthlyraw,
    i_crspmonthly,
    j_crspdaily,
    k_crspacquisitions,
    l2_ibes_eps_adj,
    l_ibes_eps_unadj,
    m_ibes_recommendations,
    n_ibes_unadjustedactuals,
    o_daily_fama_french,
    p_monthly_fama_french,
    q_marketreturns,
    r_monthlyliquidityfactor,
    s_qfactormodel,
    t_vix,
    u_gnpdeflator,
    v_tbill3m,
    w_brokerdealerleverage,
    x2_ciqcreditratings,
    x_spcreditratings,
    za_ipodates,
    zb_pin,
    zc_governanceindex,
    zd_corwinschultz,
    ze_13f,
    zf_crspibeslink,
    zg_bidasktaq,
    zh_optionmetrics,
    zi_patentcitations,
    zj_inputoutputmomentum,
    zk_customermomentum,
    zl_crspoptionmetrics,
]
