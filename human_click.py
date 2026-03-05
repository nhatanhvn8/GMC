# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import random
import time

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

_log = logging.getLogger(__name__)

_CURSOR_INJECT_JS = """
(function(){
    if(document.getElementById('__fake_cursor')) return;
    var d = document.createElement('div');
    d.id = '__fake_cursor';
    d.style.cssText = 'width:12px;height:12px;border-radius:50%;background:red;position:fixed;top:0;left:0;z-index:999999;pointer-events:none;transition:top 0.3s ease,left 0.3s ease;';
    document.body.appendChild(d);
})();
"""


def inject_fake_cursor(driver: WebDriver) -> None:
    try:
        driver.execute_script(_CURSOR_INJECT_JS)
    except Exception:
        pass


def _animate_cursor_to(driver: WebDriver, x: int, y: int) -> None:
    try:
        driver.execute_script(
            "var c=document.getElementById('__fake_cursor');"
            "if(c){c.style.left=arguments[0]+'px';c.style.top=arguments[1]+'px';}",
            x, y,
        )
    except Exception:
        pass
    time.sleep(0.35)


def human_move_and_click(driver: WebDriver, element: WebElement, pause_after: float = 0.15) -> bool:
    try:
        inject_fake_cursor(driver)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        loc = element.location
        _animate_cursor_to(driver, loc["x"], loc["y"])
        actions = ActionChains(driver)
        offset_x = random.randint(-3, 3)
        offset_y = random.randint(-3, 3)
        actions.move_to_element_with_offset(element, offset_x, offset_y)
        actions.pause(random.uniform(0.05, 0.15))
        actions.click()
        actions.perform()
        time.sleep(pause_after)
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False


def human_click_element(driver: WebDriver, element: WebElement) -> bool:
    return human_move_and_click(driver, element)


def human_click_first_visible(driver: WebDriver, by: str, selectors: list, timeout: float = 0) -> bool:
    deadline = time.time() + timeout if timeout > 0 else 0
    while True:
        for selector in selectors:
            elements = driver.find_elements(by, selector)
            for el in elements:
                if el.is_displayed() and el.is_enabled():
                    return human_move_and_click(driver, el)
        if deadline and time.time() >= deadline:
            break
        time.sleep(0.3)
    return False


def human_click_xpath(driver: WebDriver, xpaths: list, timeout: float = 0) -> bool:
    return human_click_first_visible(driver, By.XPATH, xpaths, timeout)


def human_click_css(driver: WebDriver, selectors: list, timeout: float = 0) -> bool:
    return human_click_first_visible(driver, By.CSS_SELECTOR, selectors, timeout)


def human_type(driver: WebDriver, element: WebElement, text: str, delay_min: float = 0.04, delay_max: float = 0.12) -> None:
    human_move_and_click(driver, element, pause_after=0.1)
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(delay_min, delay_max))
