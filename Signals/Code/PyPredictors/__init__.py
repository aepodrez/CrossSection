"""
PyPredictors package
Generated from Predictors .do files
"""

# Import all predictor functions
from .am import am
from .accruals import accruals
from .accrualsbm import accrualsbm
from .adexp import adexp
from .ageipo import ageipo
from .analystrevision import analystrevision
from .assetgrowth import assetgrowth
from .bm import bm
from .bmdec import bmdec
from .beta import beta
from .betaliquidityps import betaliquidityps
from .betatailrisk import betatailrisk
from .bidaskspread import bidaskspread
from .bookleverage import bookleverage
from .brandinvest import brandinvest
from .cboperprof import cboperprof
from .cf import cf
from .cpvolspread import cpvolspread
from .cash import cash
from .cashprod import cashprod
from .chassetturnover import chassetturnover
from .cheq import cheq
from .chforecastaccrual import chforecastaccrual
from .chinv import chinv
from .chinvia import chinvia
from .chnanalyst import chnanalyst
from .chnncoa import chnncoa
from .chnwc import chnwc
from .chtax import chtax
from .changeinrecommendation import changeinrecommendation
from .citationsrd import citationsrd
from .compequiss import compequiss
from .compositedebtissuance import compositedebtissuance
from .consrecomm import consrecomm
from .convdebt import convdebt
from .coskewacx import coskewacx
from .coskewness import coskewness
from .credratdg import credratdg
from .customermomentum import customermomentum
from .debtissuance import debtissuance
from .delbreadth import delbreadth
from .delcoa import delcoa
from .delcol import delcol
from .deldrc import deldrc
from .delequ import delequ
from .delfinl import delfinl
from .dellti import dellti
from .delnetfin import delnetfin
from .divinit import divinit
from .divomit import divomit
from .divseason import divseason
from .divyieldst import divyieldst
from .dolvol import dolvol
from .downrecomm import downrecomm
from .ep import ep
from .earnsupbig import earnsupbig
from .earningsconsistency import earningsconsistency
from .earningsforecastdisparity import earningsforecastdisparity
from .earningsstreak import earningsstreak
from .earningssurprise import earningssurprise
from .entmult import entmult
from .equityduration import equityduration
from .exchswitch import exchswitch
from .exclexp import exclexp
from .feps import feps
from .firmage import firmage
from .firmagemom import firmagemom
from .forecastdispersion import forecastdispersion
from .frontier import frontier
from .gp import gp
from .governance import governance
from .gradexp import gradexp
from .grltnoa import grltnoa
from .grsaletogrinv import grsaletogrinv
from .grsaletogroverhead import grsaletogroverhead
from .herf import herf
from .herfasset import herfasset
from .herfbe import herfbe
from .high52 import high52
from .io_shortinterest import io_shortinterest
from .illiquidity import illiquidity
from .indipo import indipo
from .indmom import indmom
from .indretbig import indretbig
from .intmom import intmom
from .invgrowth import invgrowth
from .investppeinv import investppeinv
from .investment import investment
from .lrreversal import lrreversal
from .leverage import leverage
from .mrreversal import mrreversal
from .ms import ms
from .maxret import maxret
from .meanrankrevgrowth import meanrankrevgrowth
from .mom12m import mom12m
from .mom12moffseason import mom12moffseason
from .mom6m import mom6m
from .mom6mjunk import mom6mjunk
from .momoffseason import momoffseason
from .momoffseason06yrplus import momoffseason06yrplus
from .momoffseason11yrplus import momoffseason11yrplus
from .momoffseason16yrplus import momoffseason16yrplus
from .momrev import momrev
from .momseason import momseason
from .momseason06yrplus import momseason06yrplus
from .momseason11yrplus import momseason11yrplus
from .momseason16yrplus import momseason16yrplus
from .momseasonshort import momseasonshort
from .momvol import momvol
from .noa import noa
from .netdebtfinance import netdebtfinance
from .netdebtprice import netdebtprice
from .netequityfinance import netequityfinance
from .netpayoutyield import netpayoutyield
from .numearnincrease import numearnincrease
from .opleverage import opleverage
from .oscore import oscore
from .oscore_q import oscore_q
from .operprof import operprof
from .operprofrd import operprofrd
from .orderbacklog import orderbacklog
from .orderbacklogchg import orderbacklogchg
from .ps import ps
from .patentsrd import patentsrd
from .payoutyield import payoutyield
from .pctacc import pctacc
from .pcttotacc import pcttotacc
from .price import price
from .probinformedtrading import probinformedtrading
from .rd import rd
from .rdability import rdability
from .rdipo import rdipo
from .rds import rds
from .rdcap import rdcap
from .rev6 import rev6
from .recomm_shortinterest import recomm_shortinterest
from .returnskew import returnskew
from .revenuesurprise import revenuesurprise
from .roe import roe
from .sp import sp
from .streversal import streversal
from .shareiss1y import shareiss1y
from .shareiss5y import shareiss5y
from .sharerepurchase import sharerepurchase
from .sharevol import sharevol
from .shortinterest import shortinterest
from .size import size
from .smileslope import smileslope
from .spinoff import spinoff
from .surpriserd import surpriserd
from .tax import tax
from .totalaccruals import totalaccruals
from .trendfactor import trendfactor
from .uprecomm import uprecomm
from .varcf import varcf
from .volmkt import volmkt
from .volsd import volsd
from .volumetrend import volumetrend
from .xfin import xfin
from .zz0_realizedvol_idiovol3f_returnskew3f import zz0_realizedvol_idiovol3f_returnskew3f
from .zz1_activism1_activism2 import zz1_activism1_activism2
from .zz1_analystvalue_aop_predictedfe_intrinsicvalue import zz1_analystvalue_aop_predictedfe_intrinsicvalue
from .zz1_ebm_bpebm import zz1_ebm_bpebm
from .zz1_fr_frbook import zz1_fr_frbook
from .zz1_intanbm_intansp_intancfp_intanep import zz1_intanbm_intansp_intancfp_intanep
from .zz1_optionvolume1_optionvolume2 import zz1_optionvolume1_optionvolume2
from .zz1_orgcap_orgcapnoadj import zz1_orgcap_orgcapnoadj
from .zz1_rio_mb_rio_disp_rio_turnover_rio_volatility import zz1_rio_mb_rio_disp_rio_turnover_rio_volatility
from .zz1_rivolspread import zz1_rivolspread
from .zz1_residualmomentum6m_residualmomentum import zz1_residualmomentum6m_residualmomentum
from .zz1_grcapx_grcapx1y_grcapx3y import zz1_grcapx_grcapx1y_grcapx3y
from .zz1_zerotrade_zerotradealt1_zerotradealt12 import zz1_zerotrade_zerotradealt1_zerotradealt12
from .zz2_abnormalaccruals_abnormalaccrualspercent import zz2_abnormalaccruals_abnormalaccrualspercent
from .zz2_announcementreturn import zz2_announcementreturn
from .zz2_betafp import zz2_betafp
from .zz2_idiovolaht import zz2_idiovolaht
from .zz2_pricedelayslope_pricedelayrsq_pricedelaytstat import zz2_pricedelayslope_pricedelayrsq_pricedelaytstat
from .zz2_betavix import zz2_betavix
from .cfp import cfp
from .dcpvolspread import dcpvolspread
from .dnoa import dnoa
from .dvolcall import dvolcall
from .dvolput import dvolput
from .fgr5yrlag import fgr5yrlag
from .hire import hire
from .iomom_cust import iomom_cust
from .iomom_supp import iomom_supp
from .realestate import realestate
from .retconglomerate import retconglomerate
from .roaq import roaq
from .sfe import sfe
from .sinalgo import sinalgo
from .skew1 import skew1
from .std_turn import std_turn
from .tang import tang

# List of all predictor functions
PREDICTOR_FUNCTIONS = [
    am,
    accruals,
    accrualsbm,
    adexp,
    ageipo,
    analystrevision,
    assetgrowth,
    bm,
    bmdec,
    beta,
    betaliquidityps,
    betatailrisk,
    bidaskspread,
    bookleverage,
    brandinvest,
    cboperprof,
    cf,
    cpvolspread,
    cash,
    cashprod,
    chassetturnover,
    cheq,
    chforecastaccrual,
    chinv,
    chinvia,
    chnanalyst,
    chnncoa,
    chnwc,
    chtax,
    changeinrecommendation,
    citationsrd,
    compequiss,
    compositedebtissuance,
    consrecomm,
    convdebt,
    coskewacx,
    coskewness,
    credratdg,
    customermomentum,
    debtissuance,
    delbreadth,
    delcoa,
    delcol,
    deldrc,
    delequ,
    delfinl,
    dellti,
    delnetfin,
    divinit,
    divomit,
    divseason,
    divyieldst,
    dolvol,
    downrecomm,
    ep,
    earnsupbig,
    earningsconsistency,
    earningsforecastdisparity,
    earningsstreak,
    earningssurprise,
    entmult,
    equityduration,
    exchswitch,
    exclexp,
    feps,
    firmage,
    firmagemom,
    forecastdispersion,
    frontier,
    gp,
    governance,
    gradexp,
    grltnoa,
    grsaletogrinv,
    grsaletogroverhead,
    herf,
    herfasset,
    herfbe,
    high52,
    io_shortinterest,
    illiquidity,
    indipo,
    indmom,
    indretbig,
    intmom,
    invgrowth,
    investppeinv,
    investment,
    lrreversal,
    leverage,
    mrreversal,
    ms,
    maxret,
    meanrankrevgrowth,
    mom12m,
    mom12moffseason,
    mom6m,
    mom6mjunk,
    momoffseason,
    momoffseason06yrplus,
    momoffseason11yrplus,
    momoffseason16yrplus,
    momrev,
    momseason,
    momseason06yrplus,
    momseason11yrplus,
    momseason16yrplus,
    momseasonshort,
    momvol,
    noa,
    netdebtfinance,
    netdebtprice,
    netequityfinance,
    netpayoutyield,
    numearnincrease,
    opleverage,
    oscore,
    oscore_q,
    operprof,
    operprofrd,
    orderbacklog,
    orderbacklogchg,
    ps,
    patentsrd,
    payoutyield,
    pctacc,
    pcttotacc,
    price,
    probinformedtrading,
    rd,
    rdability,
    rdipo,
    rds,
    rdcap,
    rev6,
    recomm_shortinterest,
    returnskew,
    revenuesurprise,
    roe,
    sp,
    streversal,
    shareiss1y,
    shareiss5y,
    sharerepurchase,
    sharevol,
    shortinterest,
    size,
    smileslope,
    spinoff,
    surpriserd,
    tax,
    totalaccruals,
    trendfactor,
    uprecomm,
    varcf,
    volmkt,
    volsd,
    volumetrend,
    xfin,
    zz0_realizedvol_idiovol3f_returnskew3f,
    zz1_activism1_activism2,
    zz1_analystvalue_aop_predictedfe_intrinsicvalue,
    zz1_ebm_bpebm,
    zz1_fr_frbook,
    zz1_intanbm_intansp_intancfp_intanep,
    zz1_optionvolume1_optionvolume2,
    zz1_orgcap_orgcapnoadj,
    zz1_rio_mb_rio_disp_rio_turnover_rio_volatility,
    zz1_rivolspread,
    zz1_residualmomentum6m_residualmomentum,
    zz1_grcapx_grcapx1y_grcapx3y,
    zz1_zerotrade_zerotradealt1_zerotradealt12,
    zz2_abnormalaccruals_abnormalaccrualspercent,
    zz2_announcementreturn,
    zz2_betafp,
    zz2_idiovolaht,
    zz2_pricedelayslope_pricedelayrsq_pricedelaytstat,
    zz2_betavix,
    cfp,
    dcpvolspread,
    dnoa,
    dvolcall,
    dvolput,
    fgr5yrlag,
    hire,
    iomom_cust,
    iomom_supp,
    realestate,
    retconglomerate,
    roaq,
    sfe,
    sinalgo,
    skew1,
    std_turn,
    tang,
]

# Total number of predictor functions
PREDICTOR_COUNT = 195

# Summary
__all__ = [
    "PREDICTOR_FUNCTIONS",
    "PREDICTOR_COUNT",
] + [
    "am", "accruals", "accrualsbm", "adexp", "ageipo", "analystrevision", "assetgrowth", "bm", "bmdec", "beta", "betaliquidityps", "betatailrisk", "bidaskspread", "bookleverage", "brandinvest", "cboperprof", "cf", "cpvolspread", "cash", "cashprod", "chassetturnover", "cheq", "chforecastaccrual", "chinv", "chinvia", "chnanalyst", "chnncoa", "chnwc", "chtax", "changeinrecommendation", "citationsrd", "compequiss", "compositedebtissuance", "consrecomm", "convdebt", "coskewacx", "coskewness", "credratdg", "customermomentum", "debtissuance", "delbreadth", "delcoa", "delcol", "deldrc", "delequ", "delfinl", "dellti", "delnetfin", "divinit", "divomit", "divseason", "divyieldst", "dolvol", "downrecomm", "ep", "earnsupbig", "earningsconsistency", "earningsforecastdisparity", "earningsstreak", "earningssurprise", "entmult", "equityduration", "exchswitch", "exclexp", "feps", "firmage", "firmagemom", "forecastdispersion", "frontier", "gp", "governance", "gradexp", "grltnoa", "grsaletogrinv", "grsaletogroverhead", "herf", "herfasset", "herfbe", "high52", "io_shortinterest", "illiquidity", "indipo", "indmom", "indretbig", "intmom", "invgrowth", "investppeinv", "investment", "lrreversal", "leverage", "mrreversal", "ms", "maxret", "meanrankrevgrowth", "mom12m", "mom12moffseason", "mom6m", "mom6mjunk", "momoffseason", "momoffseason06yrplus", "momoffseason11yrplus", "momoffseason16yrplus", "momrev", "momseason", "momseason06yrplus", "momseason11yrplus", "momseason16yrplus", "momseasonshort", "momvol", "noa", "netdebtfinance", "netdebtprice", "netequityfinance", "netpayoutyield", "numearnincrease", "opleverage", "oscore", "oscore_q", "operprof", "operprofrd", "orderbacklog", "orderbacklogchg", "ps", "patentsrd", "payoutyield", "pctacc", "pcttotacc", "price", "probinformedtrading", "rd", "rdability", "rdipo", "rds", "rdcap", "rev6", "recomm_shortinterest", "returnskew", "revenuesurprise", "roe", "sp", "streversal", "shareiss1y", "shareiss5y", "sharerepurchase", "sharevol", "shortinterest", "size", "smileslope", "spinoff", "surpriserd", "tax", "totalaccruals", "trendfactor", "uprecomm", "varcf", "volmkt", "volsd", "volumetrend", "xfin", "zz0_realizedvol_idiovol3f_returnskew3f", "zz1_activism1_activism2", "zz1_analystvalue_aop_predictedfe_intrinsicvalue", "zz1_ebm_bpebm", "zz1_fr_frbook", "zz1_intanbm_intansp_intancfp_intanep", "zz1_optionvolume1_optionvolume2", "zz1_orgcap_orgcapnoadj", "zz1_rio_mb_rio_disp_rio_turnover_rio_volatility", "zz1_rivolspread", "zz1_residualmomentum6m_residualmomentum", "zz1_grcapx_grcapx1y_grcapx3y", "zz1_zerotrade_zerotradealt1_zerotradealt12", "zz2_abnormalaccruals_abnormalaccrualspercent", "zz2_announcementreturn", "zz2_betafp", "zz2_idiovolaht", "zz2_pricedelayslope_pricedelayrsq_pricedelaytstat", "zz2_betavix", "cfp", "dcpvolspread", "dnoa", "dvolcall", "dvolput", "fgr5yrlag", "hire", "iomom_cust", "iomom_supp", "realestate", "retconglomerate", "roaq", "sfe", "sinalgo", "skew1", "std_turn", "tang"
]

print(f"📊 PyPredictors package loaded with {PREDICTOR_COUNT} predictor functions")
