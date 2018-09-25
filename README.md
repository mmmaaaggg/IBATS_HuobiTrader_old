# ABAT_trader_4_blockchain
Auto Backtest Analysis Trade Framework （简称ABAT）支持期货、数字货币进行量化交易，集成回测、分析、交易于一体。与市场上同类回测框架相比，有如下优势：

- 更加完备的支持多品种、多周期策略交易
- 对跨周期策略回测更加真实，回测模式下，将不同周期数据进行时间排序推送，从而激活对应的周期的响应函数
- 框架采用分部署架构，行情推送与主框架可分离部署，通过redis进行数据广播
- 未来将可以支持股票、期货、数字货币多种接口，同时交易

主要组件：

- 行情代理 md agent
- 交易代理 trade agent
- 行情推送 md feeder
- 回测及实时行情交易框架 ABAT

当前项目主要用于对数字货币进行自动化交易，策略分析使用。

由于精力有限，目前暂未实现前端展现。交易自动执行，通过日至可查看交易执行情况。

## 安装

系统环境要求：
> Python 3.6 
>
> MySQL 5.7 (具体配置见下文)
> 
> Redis 3.0.6 

## 配置

> config.py 配置文件

## 策略运行示例

strategy 目录下
- simple_strategy.py 实现简单均线交叉策略回测\
- file_strategy.py 调仓文件导入式的交易（实时行情）\

其他策略 coming soon

## 策略研发框架

执行策略

```python
# 参数设置
strategy_params = {}
md_agent_params_list = [
	# {
	#     'name': 'min1',
	#     'md_period': PeriodType.Min1,
	#     'instrument_id_list': ['rb1805', 'i1801'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
	#     'init_md_date_to': '2017-9-1',
	#     'dict_or_df_as_param': dict
	# },
	{
		'name': 'tick',
		'md_period': PeriodType.Tick,
		'instrument_id_list': ['ethusdt', 'eosusdt'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
	}]
run_mode_realtime_params = {
	'run_mode': RunMode.Realtime,
	'enable_timer_thread': True,
	'seconds_of_timer_interval': 15,
}
run_mode_backtest_params = {
	'run_mode': RunMode.Backtest,
	'date_from': '2017-9-4',
	'date_to': '2017-9-27',
	'init_cash': 1000000,
	'trade_mode': BacktestTradeMode.Order_2_Deal
}
# 初始化策略处理器
stghandler = StgHandlerBase.factory(
	stg_class_obj=ReadFileStg,
	strategy_params=strategy_params,
	md_agent_params_list=md_agent_params_list,
	**run_mode_realtime_params)
# 开始执行策略
stghandler.start()
# 策略执行 2 分钟后关闭
time.sleep(120)
stghandler.keep_running = False
stghandler.join()
logging.info("执行结束")
```

StgHandlerBase.factory 为工厂方法，用于产生策略执行对象实力

```Python
def factory(stg_class_obj: StgBase.__class__, strategy_params, md_agent_params_list, run_mode: RunMode, **run_mode_params):
    """
    建立策略对象
    建立数据库相应记录信息
    根据运行模式（实时、回测）：选择相应的md_agent以及trade_agent
    :param stg_class_obj: 策略类型 StgBase 的子类
    :param strategy_params: 策略参数，策略对象初始化是传入参数使用
    :param md_agent_params_list: 行情代理（md_agent）参数，支持同时订阅多周期、多品种，例如同时订阅 [ethusdt, eosusdt] 1min 行情、[btcusdt, ethbtc] tick 行情
    :param run_mode: 运行模式 RunMode.Realtime  或 RunMode.Backtest
    :param run_mode_params: 运行参数，
    :return: 策略处理对象实力
    """
```

## 欢迎赞助

#### 微信

![微信支付](https://github.com/mmmaaaggg/ABAT_trader_4_blockchain/blob/master/mass/webchat_code200.png?raw=true)

#### 支付宝

![微信支付](https://github.com/mmmaaaggg/ABAT_trader_4_blockchain/blob/master/mass/alipay_code200.png?raw=true)

#### 微信打赏（￥10）

![微信打赏](https://github.com/mmmaaaggg/ABAT_trader_4_blockchain/blob/master/mass/dashang_code200.png?raw=true)

## MySQL 配置方法

 1. Ubuntu 18.04 环境下安装 MySQL，5.7
 
    ```bash
    sudo apt install mysql-server
    ```
 2. 默认情况下，没有输入用户名密码的地方，因此，安装完后需要手动重置Root密码，方法如下：

    ```bash
    cd /etc/mysql/debian.cnf
    sudo more debian.cnf
    ```
    出现类似这样的东西
    > \# Automatically generated for Debian scripts. DO NOT TOUCH!
    [client]
    host     = localhost
    user     = debian-sys-maint
    password = j1bsABuuDRGKCV5s
    socket   = /var/run/mysqld/mysqld.sock
    [mysql_upgrade]
    host     = localhost
    user     = debian-sys-maint
    password = j1bsABuuDRGKCV5s
    socket   = /var/run/mysqld/mysqld.sock

    以debian-sys-maint为用户名登录，密码就是debian.cnf里那个 password = 后面的东西。
    使用mysql -u debian-sys-maint -p 进行登录。
    进入mysql之后修改MySQL的密码，具体的操作如下用命令：
    ```mysql
    use mysql;
    
    update user set authentication_string=PASSWORD("Dcba4321") where user='root';
    
    update user set plugin="mysql_native_password"; 
     
    flush privileges;
    ```
 3. 然后就可以用过root用户登陆了

    ```bash
    mysql -uroot -p
    ```

 4. 创建用户 mg 默认密码 Abcd1234

    ```mysql
    CREATE USER 'mg'@'%' IDENTIFIED BY 'Abcd1234';
    ```
 5. 创建数据库 bc_md

    ```mysql
    CREATE DATABASE `abat` default charset utf8 collate utf8_general_ci;
    ```
 6. 授权

    ```mysql
    grant all privileges on abat.* to 'mg'@'localhost' identified by 'Abcd1234'; 
    
    flush privileges; #刷新系统权限表
    ```
