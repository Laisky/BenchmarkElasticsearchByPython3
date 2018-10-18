"""
@author: Laisky

Need Python v3.6

Need Packages:

- aiohttp
- kipp

Run:
::
    python benchmark.py --addr=localhost:9200 --n_conn=100 --n_procs=7 --n_docus=30000000

"""
import asyncio
import datetime
import json
import logging
from random import randint, random, choice, randrange
import time
from string import ascii_letters
from multiprocessing import RLock
from multiprocessing.sharedctypes import Value
from ctypes import c_int32
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError

from kipp.options import opt
from kipp.decorator import debug_wrapper
from kipp.utils import setup_logger
from kipp.aio import Event
import aiohttp


HEADERS = {'content-type': 'application/json'}


async def post_data(s, task_q):
    opt.logger.debug('post_data')
    url = '{}{}/{}'.format(opt.addr, opt.index, opt.type)
    while not (opt.producer_is_set.is_set() and task_q.empty()):
        data = await task_q.get()
        try:
            async with s.post(url, data=data, headers=HEADERS, timeout=3) as resp:
                cont = await resp.text()
                assert resp.status == 201
        except TimeoutError:
            opt.set_option('n_err_docus', opt.n_err_docus+1)
            continue
        except Exception:
            opt.set_option('n_err_docus', opt.n_err_docus+1)
            if (resp.status == 503  # Nginx load balance error
                    or resp.status == 429):  # elasticsearch queue overflow
                continue

            opt.logger.debug('Error for data {}, resp: {}'.format(data, cont), exc_info=True)
        else:
            opt.set_option('n_current_docus', opt.n_current_docus+1)


async def create_index(s):
    opt.logger.info('create index for %s/%s', opt.index, opt.type)
    url = '{}/{}/_mapping/{}'.format(opt.addr, opt.index, opt.type)
    data = json.dumps({
        "properties": {
            # "@timestamp": {
            #     "type": "date",
            #     "format": "yyyy-MM-dd HH:mm:ss"
            # },
            "level": {
                "type": "keyword"
            },
            "log": {
                "type": "string"
            }
        }
    })

    async with s.put(url, data=data, headers=HEADERS, timeout=3) as resp:
        if resp.status//100 == 2:
            raise (await resp.text())


def generate_level():
    if random() < 0.8:
        return 'INFO'
    else:
        return 'ERROR'


def generate_time():
    return (datetime.datetime.now() + datetime.timedelta(hours=randint(-720, 720))).strftime('%Y-%m-%dT%H:%M:%S')


def generate_message():
    msg_size = int(opt.msg_size / 1000 * randrange(900, 1100))
    return ''.join([choice(ascii_letters) for _ in range(msg_size)])


async def produce_tasks(task_q):
    opt.logger.debug('produce_tasks')
    for _ in range(opt.n_docus):
        l = generate_level()
        t = generate_time()
        m = generate_message()
        await task_q.put(json.dumps({
            'level': l,
            '@timestamp': t,
            'log': '{} {} {}'.format(t, l, m),
        }))


async def runner():
    opt.logger.debug('runner')
    opt.set_option('producer_is_set', Event())
    opt.set_option('post_data_is_set', Event())
    task_q = asyncio.Queue(maxsize=100)
    async with aiohttp.ClientSession() as s:
        await create_index(s)
        workers = [asyncio.ensure_future(post_data(s, task_q)) for _ in range(opt.n_conn)]
        task_producer = asyncio.ensure_future(produce_tasks(task_q))
        stats_runner = asyncio.ensure_future(calculate_stats(task_q))
        await task_producer
        opt.producer_is_set.set()
        await asyncio.wait(workers)
        opt.post_data_is_set.set()
        await stats_runner


async def calculate_stats(task_q):
    while not (opt.post_data_is_set.is_set() and opt.n_current_docus+opt.n_err_docus==0):
        opt.global_n_docus.value += opt.n_current_docus
        opt.global_n_err_docus.value += opt.n_err_docus
        opt.set_option('n_current_docus', 0)
        opt.set_option('n_err_docus', 0)
        opt.logger.debug('submit stats to manager, current queue %s/100', task_q.qsize())
        await asyncio.sleep(1)


def setup_settings():
    opt.set_option('docu_lock', RLock())
    opt.set_option('err_docu_lock', RLock())
    opt.set_option('executor', ProcessPoolExecutor(max_workers=opt.n_procs+1))
    opt.set_option('global_n_docus', Value(c_int32, 0, lock=opt.docu_lock))
    opt.set_option('global_n_err_docus', Value(c_int32, 0, lock=opt.err_docu_lock))
    opt.set_option('global_is_done', Value(c_int32, 0))
    opt.set_option('global_last_time', time.time())
    opt.set_option('logger', setup_logger('es_benchmark'))
    if opt.debug:
        opt.logger.setLevel(logging.DEBUG)
    opt.set_option('n_current_docus', 0)
    opt.set_option('n_err_docus', 0)
    opt.set_option('last_time', time.time())

    opt.logger.info(f"set elasticsearch: {opt.addr}")
    opt.logger.info('will insert %s documents with %s bytes for %s connections, %s processes',
                    opt.n_docus * opt.n_procs, opt.msg_size, opt.n_conn * opt.n_procs, opt.n_procs)


def main_for_each_process(i_procs):
    ioloop = asyncio.get_event_loop()
    f = asyncio.ensure_future(runner())
    ioloop.run_until_complete(f)
    opt.logger.info('main_for_each_process %s result: %s', i_procs, f.result())


def calculate_global_stats():
    last_t = time.time()
    while not opt.global_is_done.value:
        t = time.time()
        if t - last_t < 10:
            time.sleep(1)
            continue

        last_t = t
        opt.docu_lock.acquire()
        speed = opt.global_n_docus.value / (t - opt.global_last_time)
        opt.global_n_docus.value = 0
        opt.docu_lock.release()

        opt.err_docu_lock.acquire()
        err_speed = opt.global_n_err_docus.value / (t - opt.global_last_time)
        opt.global_n_err_docus.value = 0
        opt.err_docu_lock.release()

        opt.set_option('global_last_time', t)
        opt.logger.info('%s docus/sec, %s err/sec', round(speed, 1), round(err_speed, 1))


@debug_wrapper
def main():
    fs = [opt.executor.submit(main_for_each_process, i) for i in range(opt.n_procs)]
    futu = opt.executor.submit(calculate_global_stats)
    as_completed(fs)
    try:
        list([f.result() for f in fs])
    except Exception:
        opt.logger.exception('main_for_each_process error:')
        raise
    finally:
        opt.global_is_done.value = 1
        futu.result()


if __name__ == '__main__':
    opt.add_argument('--addr', type=str, default="localhost:9200", help="elasticsearch http api")
    opt.add_argument('--n_docus', type=int, default=100000, help="number of documents that will be insert into ES each process")
    opt.add_argument('--n_procs', type=int, default=1, help="number of processes")
    opt.add_argument('--n_conn', type=int, default=10, help="number of connections for each process")
    opt.add_argument('--msg_size', type=int, default=500)
    opt.add_argument('--index', type=str, default='logs', help="index's name")
    opt.add_argument('--type', type=str, default='app_logs', help="type's name")
    opt.add_argument('--debug', action='store_true', default=False)
    opt.parse_args()
    setup_settings()
    main()
