import sys
import time
import os
import asyncio
from aiogram import Bot, Dispatcher, executor, types
from aiogram.bot.api import TelegramAPIServer
from aiogram.contrib.fsm_storage.memory import MemoryStorage

import logging
import subprocess
from logging import handlers
from pprint import pprint
import re
from requests import get
from dotenv import load_dotenv, find_dotenv


# print(API_TOKEN)
bot = Bot(token='5408841853:AAFTIR9Gn3JBwft0OdOhs6tr2dVpDFTMeN4')
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

async def logout(): 
	res = await bot.log_out()
	print(res)

asyncio.run(logout())

# bot.log_out()
