import logging
import time
from functools import wraps


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            timer = start_sleep_time
            state = True

            try:
                logging.debug(f"Inside backoff - timer is {timer}")
                return func(*args, **kwargs)
                timer = start_sleep_time

            except Exception as e:
                logging.info(f"Cought exeption {e}")
                if timer < border_sleep_time:
                        time.sleep(start_sleep_time)
                        timer = timer * 2 * factor
                else:
                    time.sleep(border_sleep_time)

        return inner

    return func_wrapper
