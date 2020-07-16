import pytest
from datetime import datetime as dt
from datetime import timedelta
from babel.babel_utils import ThrottledRequester

def test_throttling():
    """Call a quick-returning service, but include a throttle of 1/2 second (500 ms).
    This should end up taking just a bit over 500 ms.  The service being called
    just returns an empty json.  It's not immediate, but it returns quick enough that
    a 1/2 second throttle will be invoked.
    The test is a little goofy, and the half_sec_plus number is made up"""
    tr = ThrottledRequester(500)
    now = dt.now()
    response,throttle1 = tr.get('http://www.mocky.io/v2/5df243b23100007f009a31b0')
    response,throttle2 = tr.get('http://www.mocky.io/v2/5df243b23100007f009a31b0')
    later = dt.now()
    runtime = later - now
    half_sec = timedelta(milliseconds = 500)
    half_sec_plus = timedelta(milliseconds = 810)
    assert not throttle1 #don't throttle the first time through
    assert throttle2     #do throttle the second time through
    assert runtime > half_sec
    assert runtime < half_sec_plus

def test_no_throttling():
    """Call a slow-returning service, but include a throttle of 1/2 second (500 ms).
    Because the service being called takes longer than the throttle value, we should
    not wait the second time through.  The mocky service lets us specify a delay time. """
    tr = ThrottledRequester(500)
    now = dt.now()
    response,throttle1 = tr.get('http://www.mocky.io/v2/5df243b23100007f009a31b0?mocky-delay=600ms')
    response,throttle2 = tr.get('http://www.mocky.io/v2/5df243b23100007f009a31b0')
    later = dt.now()
    runtime = later - now
    lower_bound = timedelta(milliseconds = 600)
    assert not throttle1 #don't throttle the first time through
    assert not throttle2 #Nor the second time, because the call itself took longer than the throttle time
    assert runtime > lower_bound #make sure we actually delayed

