#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys
import os
import time
import threading
import xlsxwriter
import copy
import locale
import math 
import pprint

from datetime import date, datetime, timedelta
from pathlib import Path
from PySide2.QtSql import QSqlDatabase, QSqlQuery
from PySide2.QtCore import QThread,QDateTime,QSettings,Slot,QTime,QLockFile,QFile,QTextStream,QLocale,Qt
from PySide2.QtGui import QIcon
from PySide2.QtWidgets import (QApplication, QCheckBox, QComboBox, QDateTimeEdit, QDateEdit,QSystemTrayIcon,QMenu,QAction,QFileDialog,
        QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit,
        QPushButton, QStyleFactory, QTabWidget, QVBoxLayout, QWidget,QMessageBox,QTimeEdit)

QLocale.setDefault(QLocale(QLocale.English, QLocale.UnitedStates))

settings = QSettings("config.ini", QSettings.IniFormat)
configSite = settings.value('Site/name', "") 
configHost = settings.value('Postgresql/host', "")
configDatabase = settings.value('Postgresql/database', "")
configDatabase = configDatabase.replace("\\\\","\\")
configPort = settings.value('Postgresql/port', 5433)
configUsername = settings.value('Postgresql/username', "postgres")
configPassword = settings.value('Postgresql/password', "")
configOutput = settings.value('Output/folder', "")    
configCrontime = settings.value('Output/crontime', "") 
configIx = settings.value('Ix/Thoeng', 0.00)

if(configSite  == 'MAE CHAN'):
    configIx = settings.value('Ix/MaeChan', "") 

keytagname = [
    ["OUT01",'OUT01_MMXU1PPVPhsABIMag','OUT01_MMXU1PPVPhsBCIMag','OUT01_MMXU1PPVPhsCAIMag',
    'OUT01_MMXU1APhsAIMag', 'OUT01_MMXU1APhsBIMag', 'OUT01_MMXU1APhsCIMag',
    'OUT01_MMXU1TotWInstMag','OUT01_MMXU1TotVArInstMag','OUT01_MMXU1TotPFMag','OUT01_CSWI2PosStVal','OUT01'],
            
    ["OUT02",'OUT02_MMXU1PPVPhsABIMag','OUT02_MMXU1PPVPhsBCIMag','OUT02_MMXU1PPVPhsCAIMag',
    'OUT02_MMXU1APhsAIMag', 'OUT02_MMXU1APhsBIMag', 'OUT02_MMXU1APhsCIMag',
    'OUT02_MMXU1TotWInstMag','OUT02_MMXU1TotVArInstMag','OUT02_MMXU1TotPFMag','OUT02_CSWI2PosStVal','OUT02'],
    
    ["OUT03",'OUT03_MMXU1PPVPhsABIMag','OUT03_MMXU1PPVPhsBCIMag','OUT03_MMXU1PPVPhsCAIMag',
    'OUT03_MMXU1APhsAIMag', 'OUT03_MMXU1APhsBIMag', 'OUT03_MMXU1APhsCIMag',
    'OUT03_MMXU1TotWInstMag','OUT03_MMXU1TotVArInstMag','OUT03_MMXU1TotPFMag','OUT03_CSWI2PosStVal','OUT03'],
    
    ["OUT04",'OUT04_MMXU1PPVPhsABIMag','OUT04_MMXU1PPVPhsBCIMag','OUT04_MMXU1PPVPhsCAIMag',
    'OUT04_MMXU1APhsAIMag', 'OUT04_MMXU1APhsBIMag', 'OUT04_MMXU1APhsCIMag',
    'OUT04_MMXU1TotWInstMag','OUT04_MMXU1TotVArInstMag','OUT04_MMXU1TotPFMag','OUT04_CSWI2PosStVal','OUT04'],
    
    ["OUT05",'OUT05_MMXU1PPVPhsABIMag','OUT05_MMXU1PPVPhsBCIMag','OUT05_MMXU1PPVPhsCAIMag',
     'OUT05_MMXU1APhsAIMag', 'OUT05_MMXU1APhsBIMag', 'OUT05_MMXU1APhsCIMag',
    'OUT05_MMXU1TotWInstMag','OUT05_MMXU1TotVArInstMag','OUT05_MMXU1TotPFMag','OUT05_CSWI2PosStVal','OUT05'],
    
    ["OUT06",'OUT06_MMXU1PPVPhsABIMag','OUT06_MMXU1PPVPhsBCIMag','OUT06_MMXU1PPVPhsCAIMag',
    'OUT06_MMXU1APhsAIMag', 'OUT06_MMXU1APhsBIMag', 'OUT06_MMXU1APhsCIMag',
    'OUT06_MMXU1TotWInstMag','OUT06_MMXU1TotVArInstMag','OUT06_MMXU1TotPFMag','OUT06_CSWI2PosStVal','OUT06'],
    
    ["OUT07",'OUT07_MMXU1PPVPhsABIMag','OUT07_MMXU1PPVPhsBCIMag','OUT07_MMXU1PPVPhsCAIMag',
    'OUT07_MMXU1APhsAIMag', 'OUT07_MMXU1APhsBIMag', 'OUT07_MMXU1APhsCIMag',
    'OUT07_MMXU1TotWInstMag','OUT07_MMXU1TotVArInstMag','OUT07_MMXU1TotPFMag','OUT07_CSWI2PosStVal','OUT07'],
    
    ["OUT08",'OUT08_MMXU1PPVPhsABIMag','OUT08_MMXU1PPVPhsBCIMag','OUT08_MMXU1PPVPhsCAIMag',
    'OUT08_MMXU1APhsAIMag', 'OUT08_MMXU1APhsBIMag', 'OUT08_MMXU1APhsCIMag',
    'OUT08_MMXU1TotWInstMag','OUT08_MMXU1TotVArInstMag','OUT08_MMXU1TotPFMag','OUT08_CSWI2PosStVal','OUT08'],
    
    ["OUT09",'OUT09_MMXU1PPVPhsABIMag','OUT09_MMXU1PPVPhsBCIMag','OUT09_MMXU1PPVPhsCAIMag',
    'OUT09_MMXU1APhsAIMag', 'OUT09_MMXU1APhsBIMag', 'OUT09_MMXU1APhsCIMag',
    'OUT09_MMXU1TotWInstMag','OUT09_MMXU1TotVArInstMag','OUT09_MMXU1TotPFMag','OUT09_CSWI2PosStVal','OUT09'],

    ["OUT10",'OUT10_MMXU1PPVPhsABIMag','OUT10_MMXU1PPVPhsBCIMag','OUT10_MMXU1PPVPhsCAIMag',
    'OUT10_MMXU1APhsAIMag', 'OUT10_MMXU1APhsBIMag', 'OUT10_MMXU1APhsCIMag',
    'OUT10_MMXU1TotWInstMag','OUT10_MMXU1TotVArInstMag','OUT10_MMXU1TotPFMag','OUT10_CSWI2PosStVal','OUT10'],
    
    ["BS01",'BS01_MMXU1PPVPhsABIMag','BS01_MMXU1PPVPhsBCIMag','BS01_MMXU1PPVPhsCAIMag',
    'BS01_MMXU1APhsAIMag', 'BS01_MMXU1APhsBIMag', 'BS01_MMXU1APhsCIMag',
    'BS01_MMXU1TotWInstMag','BS01_MMXU1TotVArInstMag','BS01_MMXU1TotPFMag','BS01_CSWI2PosStVal','BUS SECTION'],
        
    ["CAP01",'CAP01_MMXU1PPVPhsABIMag','CAP01_MMXU1PPVPhsBCIMag','CAP01_MMXU1PPVPhsCAIMag',
    'CAP01_MMXU1APhsAIMag', 'CAP01_MMXU1APhsBIMag', 'CAP01_MMXU1APhsCIMag',
    'CAP01_MMXU1TotWInstMag','CAP01_MMXU1TotVArInstMag','CAP01_MMXU1TotPFMag','CAP01_CSWI2PosStVal','CAPACITOR NO.1'],
    
    ["CAP02",'CAP02_MMXU1PPVPhsABIMag','CAP02_MMXU1PPVPhsBCIMag','CAP02_MMXU1PPVPhsCAIMag',
    'CAP02_MMXU1APhsAIMag', 'CAP02_MMXU1APhsBIMag', 'CAP02_MMXU1APhsCIMag',
    'CAP02_MMXU1TotWInstMag','CAP02_MMXU1TotVArInstMag','CAP02_MMXU1TotPFMag','CAP02_CSWI2PosStVal','CAPACITOR NO.2'],
                
    ["TS01",'TS01_MMXU1PPVPhsABIMag','TS01_MMXU1PPVPhsBCIMag','TS01_MMXU1PPVPhsCAIMag',
    'TS01_MMXU1APhsAIMag', 'TS01_MMXU1APhsBIMag', 'TS01_MMXU1APhsCIMag',
    'TS01_MMXU1TotWInstMag','TS01_MMXU1TotVArInstMag','TS01_MMXU1TotPFMag','TS01_CSWI2PosStVal','STATION SERVICE TRANSFORMER NO.1'],
    
    ["TS02",'TS02_MMXU1PPVPhsABIMag','TS02_MMXU1PPVPhsBCIMag','TS02_MMXU1PPVPhsCAIMag',
    'TS02_MMXU1APhsAIMag', 'TS02_MMXU1APhsBIMag', 'TS02_MMXU1APhsCIMag',
    'TS02_MMXU1TotWInstMag','TS02_MMXU1TotVArInstMag','TS02_MMXU1TotPFMag','TS02_CSWI2PosStVal','STATION SERVICE TRANSFORMER NO.2'],
                
    ["INC01",'INC01_MMXU1PPVPhsABIMag','INC01_MMXU1PPVPhsBCIMag','INC01_MMXU1PPVPhsCAIMag',
    'INC01_MMXU1APhsAIMag', 'INC01_MMXU1APhsBIMag', 'INC01_MMXU1APhsCIMag',
    'INC01_MMXU1TotWInstMag','INC01_MMXU1TotVArInstMag','INC01_MMXU1TotPFMag','INC01_CSWI2PosStVal','INCOMING NO.1'],
    
    ["INC02",'INC02_MMXU1PPVPhsABIMag','INC02_MMXU1PPVPhsBCIMag','INC02_MMXU1PPVPhsCAIMag',
    'INC02_MMXU1APhsAIMag', 'INC02_MMXU1APhsBIMag', 'INC02_MMXU1APhsCIMag',
    'INC02_MMXU1TotWInstMag','INC02_MMXU1TotVArInstMag','INC02_MMXU1TotPFMag','INC02_CSWI2PosStVal','INCOMING NO.2']
]

if(configSite  == 'MAE CHAN'):
    
    keytagname.append(["L1M1",'L1M1_MMXU1PPVPhsABIMag','L1M1_MMXU1PPVPhsBCIMag','L1M1_MMXU1PPVPhsCAIMag',
    'L1M1_MMXU1APhsAIMag', 'L1M1_MMXU1APhsBIMag', 'L1M1_MMXU1APhsCIMag',
    'L1M1_MMXU1TotWInstMag','L1M1_MMXU1TotVArInstMag','L1M1_MMXU1TotPFMag','L1M1_CSWI2PosStVal','LINE NO.1'])

    keytagname.append(["L2BCU",'L2BCU_MMXU1PPVPhsABIMag','L2BCU_MMXU1PPVPhsBCIMag','L2BCU_MMXU1PPVPhsCAIMag',
    'L2BCU_MMXU1APhsAIMag', 'L2BCU_MMXU1APhsBIMag', 'L2BCU_MMXU1APhsCIMag',
    'L2BCU_MMXU1TotWInstMag','L2BCU_MMXU1TotVArInstMag','L2BCU_MMXU1TotPFMag','L2BCU_CSWI2PosStVal','LINE NO.2'])
    
    keytagname.append(["L3BCU",'L3BCU_MMXU1PPVPhsABIMag','L3BCU_MMXU1PPVPhsBCIMag','L3BCU_MMXU1PPVPhsCAIMag',
    'L3BCU_MMXU1APhsAIMag', 'L3BCU_MMXU1APhsBIMag', 'L3BCU_MMXU1APhsCIMag',
    'L3BCU_MMXU1TotWInstMag','L3BCU_MMXU1TotVArInstMag','L3BCU_MMXU1TotPFMag','L3BCU_CSWI2PosStVal','LINE NO.3']) 

keytagid = copy.deepcopy(keytagname)
setzero = []
for i in keytagname:
    setzero.append((settings.value('Setzero/'+i[0], 0.00)))

class SystemTrayIcon(QSystemTrayIcon):

    def __init__(self, icon, parent=None):
        super(SystemTrayIcon, self).__init__(icon, parent)
        app.setQuitOnLastWindowClosed(False)
        menu = QMenu(parent)
        reopenAction = menu.addAction("Reports")
        exitAction = menu.addAction("Exit")
        reopenAction.triggered.connect(self.reopen)
        self.setContextMenu(menu)
        self.thread = Thread(self)
        exitAction.triggered.connect(self.thread.stop)
        self.thread.start()

    def stopThread(self):
        self.thread.stop2()
        
    def startThread(self):
        self.thread.start()
        
    def updateCrontime(self):
        self.thread.update_crontime()
        
    def reopen(self):        
        window.show()

class MainWindow(QWidget):
    
    def __init__(self):
        super(MainWindow, self).__init__()
        self.db = QSqlDatabase.addDatabase('QPSQL','WindowConnect')
        self.db.setHostName(configHost)
        self.db.setDatabaseName(configDatabase)
        self.db.setPort(int(configPort))
        self.db.setUserName(configUsername)
        self.db.setPassword(configPassword)
        ok = self.db.open()
        self.createReportFolder()
        self.initUI()


    def initUI(self):

        self.originalPalette = QApplication.palette()
        self.timeedit = QTimeEdit(self)
        self.timeedit.setTime(QTime.fromString(configCrontime))
        self.timeedit.setDisplayFormat('hh:mm')
                
        self.styleLabel = QLabel("Generate a report every:")

        self.useStylePaletteCheckBox = QCheckBox("&Generate a report every:")
        self.useStylePaletteCheckBox.setChecked(True)
        self.useStylePaletteCheckBox.setVisible(False)

        self.timeedit.setStyleSheet("QTimeEdit{color: green;}") 
        self.timeedit.setEnabled(False)    

        self.useStylePaletteCheckBox.toggled.connect(self.do_something)
        self.createBottomLeftTabWidget()
        
        # print(f"xxxx -> {self.configSite}")
        # print("configIx",configIx)

        topLayout = QHBoxLayout()
        site = QLabel("Site:")
        station = QLabel(configSite + " Substation")
        topLayout.addWidget(site)
        topLayout.addWidget(station)
        topLayout.addStretch(1)
        topLayout.addWidget(self.styleLabel)
        topLayout.addWidget(self.useStylePaletteCheckBox)
        topLayout.addWidget(self.timeedit)

        mainLayout = QGridLayout()
        mainLayout.addLayout(topLayout, 0, 0, 1, 2)        
        mainLayout.addWidget(self.bottomLeftTabWidget, 1, 0)
        
        self.setLayout(mainLayout)
        self.setWindowTitle("PEA Monitoring Reports")        
        self.tray_icon = SystemTrayIcon(QIcon('report.png'), self)      
        self.tray_icon.show()
        self.getKeyTag()
        QApplication.setStyle(QStyleFactory.create('Fusion'))
   
    def getKeyTag(self):
    
        self.db3 = QSqlDatabase.addDatabase('QPSQL','KeyTagConnect')
        self.db3.setHostName(configHost)
        self.db3.setDatabaseName(configDatabase)
        self.db3.setPort(int(configPort))
        self.db3.setUserName(configUsername)
        self.db3.setPassword(configPassword)

        file = open('keytagid.log','w+')
        self.db3.open()

        for i in range(len(keytagname)):

            for j in range(1,len(keytagname[i])-1):
                a_list = []
                # print(keytagid[i][j])
                query = QSqlQuery(self.db3)
                query.exec_('select "KeyTag" from "DataPointDetail" where "Name" = \''+keytagname[i][j]+'\'')
                while query.next():
                    a_list.append(query.value('KeyTag'))
                keytagid[i][j] = a_list
                file.write('Name: '+str(keytagname[i][j])+' | KeyTag: '+ ",".join(str(x) for x in a_list)+'\n')
        self.db3.close()
        file.close()
    
    def createBottomLeftTabWidget(self):
        self.bottomLeftTabWidget = QTabWidget()
        
        self.reportname = ["OUT01", "OUT02", "OUT03","OUT04", "OUT05", "OUT06","OUT07", "OUT08", "OUT09", "OUT10",
                           "BS01", "CAP01", "CAP02", "TS01", "TS02", "INC01", "INC02"]
        
        if(configSite  == 'MAE CHAN'):
            self.reportname.append("L1M1")
            self.reportname.append("L2BCU")            
            self.reportname.append("L3BCU")
                    
        self.dailyReports = QComboBox()
        self.dailyReports.addItems(self.reportname)

        tab1 = QWidget()
        tab1hbox = QVBoxLayout()
        self.groupbox = QGroupBox("Daily")
        
        vbox = QGridLayout()
        self.groupbox.setLayout(vbox)

        self.defaultDailyButton = QPushButton("Query")        
        self.defaultDailyButton.clicked.connect(self.createDailyReport)
        self.dateTimeStartEdit = QDateEdit()
        self.dateTimeStartEdit.setDisplayFormat("dd/MM/yyyy")
        self.dateTimeStartEdit.setDateTime(datetime.now())

        self.dateTimeFinishEdit = QDateEdit()
        self.dateTimeFinishEdit.setDisplayFormat("dd/MM/yyyy") 
        self.dateTimeFinishEdit.setDateTime(QDateTime.currentDateTime())
        
        vbox.addWidget(self.dailyReports, 1, 0)
        vbox.addWidget(self.dateTimeStartEdit, 1, 1)
        vbox.addWidget(self.dateTimeFinishEdit, 1, 2)
        vbox.addWidget(self.defaultDailyButton, 1, 3)
        tab1hbox.addWidget(self.groupbox)

        groupbox2 = QGroupBox("Monthly")
        
        self.monthlyReports = QComboBox()
        self.monthlyReports.addItems(self.reportname)
    
        dt = datetime.today()
    
        self.combo = QComboBox(self)
        self.combo.addItem("January")
        self.combo.addItem("February")
        self.combo.addItem("March")
        self.combo.addItem("April")
        self.combo.addItem("May")
        self.combo.addItem("June")
        self.combo.addItem("July")
        self.combo.addItem("August")
        self.combo.addItem("September")
        self.combo.addItem("October")       
        self.combo.addItem("November")
        self.combo.addItem("December")
        
        self.combo.setCurrentIndex(dt.month-1)
        
        self.combo2 = QComboBox(self)
        self.combo2.addItem("2020")
        self.combo2.addItem("2021")
        self.combo2.addItem("2022")
        self.combo2.addItem("2023")
        self.combo2.addItem("2024")
        self.combo2.addItem("2025")
        self.combo2.addItem("2026")
        self.combo2.addItem("2027")
        self.combo2.addItem("2028")
        self.combo2.addItem("2029")
        self.combo2.addItem("2030")

        self.combo2.setCurrentText(str(dt.year))

        defaultMonthlyButton = QPushButton("Query")
        defaultMonthlyButton.clicked.connect(self.createMonthlyReport)  
        
        group2vbox = QGridLayout()
        space2 = QLabel(' ')
        group2vbox.addWidget(self.monthlyReports, 1, 0) 
        group2vbox.addWidget(self.combo, 1, 1)
        group2vbox.addWidget(self.combo2, 1, 2)
        group2vbox.addWidget(defaultMonthlyButton, 1, 3)
        groupbox2.setLayout(group2vbox)
        tab1hbox.addWidget(groupbox2)  
      
        groupbox3 = QGroupBox("Yearly")
        defaultYearlyButton = QPushButton("Query")
        defaultYearlyButton.clicked.connect(self.createYearlyReport)  
        
        self.combo3 = QComboBox(self)
        self.combo3.addItem("2020")
        self.combo3.addItem("2021")
        self.combo3.addItem("2022")
        self.combo3.addItem("2023")
        self.combo3.addItem("2024")
        self.combo3.addItem("2025")
        self.combo3.addItem("2026")
        self.combo3.addItem("2027")
        self.combo3.addItem("2028")
        self.combo3.addItem("2029")
        self.combo3.addItem("2030")    

        self.combo3.setCurrentText(str(dt.year))

        group3vbox = QGridLayout()

        space4 = QLabel(' ')
        
        self.yearlyReports = QComboBox()
        self.yearlyReports.addItems(self.reportname)
        
        group3vbox.addWidget(self.yearlyReports, 1, 0)
        group3vbox.addWidget(self.combo3, 1, 1)
        group3vbox.addWidget(space4, 1, 2)
        group3vbox.addWidget(defaultYearlyButton, 1, 3)

        groupbox3.setLayout(group3vbox)
        tab1hbox.addWidget(groupbox3)      
        tab1.setLayout(tab1hbox)
    
        tab2 = QWidget()
        tab2hbox = QVBoxLayout()
        groupbox = QGroupBox("Database")
        tab2hbox.addWidget(groupbox)      
      
        pgUsernameTxt = QLabel('Username')
        pgPasswordTxt = QLabel('Password')
        pgHostTxt = QLabel('Host')
        pgPortTxt = QLabel('Port')
        pgDBNameTxt = QLabel('DB Name')
      
        self.pgUsernameEdit = QLineEdit()
        self.pgPasswordEdit = QLineEdit()
        self.pgHostEdit = QLineEdit()
        self.pgPortEdit = QLineEdit()
        self.pgDatabaseEdit = QLineEdit()
      
        defaultTestButton = QPushButton("Test connection")
        defaultTestButton.clicked.connect(self.testConnection)  
        
        grid = QGridLayout()        
        grid.setSpacing(10)
        grid.addWidget(pgUsernameTxt, 1, 0)
        grid.addWidget(self.pgUsernameEdit, 1, 1)
        grid.addWidget(pgPasswordTxt, 1, 2)
        grid.addWidget(self.pgPasswordEdit, 1, 3)
        grid.addWidget(pgHostTxt, 2, 0)
        grid.addWidget(self.pgHostEdit, 2, 1)
        grid.addWidget(pgPortTxt, 2, 2)
        grid.addWidget(self.pgPortEdit, 2, 3)
        grid.addWidget(pgDBNameTxt, 3, 0)
        grid.addWidget(self.pgDatabaseEdit, 3, 1,1,2)
        grid.addWidget(defaultTestButton, 3,3)
        groupbox.setLayout(grid)
        
        groupbox2 = QGroupBox("Output Folder")
        tab2hbox.addWidget(groupbox2)  

        grid2 = QGridLayout()
        self.FolderEdit = QLineEdit()
        defaultBrowseButton = QPushButton("Browse Directory")   
        defaultBrowseButton.clicked.connect(self.openDirectoryDialog)        
        grid2.addWidget(self.FolderEdit, 1, 0)
        grid2.addWidget(defaultBrowseButton, 1, 1)               
        groupbox2.setLayout(grid2)
            
        tab2.setLayout(tab2hbox)
      
        self.pgUsernameEdit.setText(configUsername)
        self.pgPasswordEdit.setText(configPassword)
        self.pgHostEdit.setText(configHost)
        self.pgPortEdit.setText(configPort)
        self.pgDatabaseEdit.setText(configDatabase)
        self.FolderEdit.setText(configOutput)
        self.bottomLeftTabWidget.addTab(tab1, "&Reports")
        self.bottomLeftTabWidget.addTab(tab2, "&Configuration")

    def do_something(self):
        
        if self.useStylePaletteCheckBox.isChecked() == True: 
            self.timeedit.setStyleSheet("QTimeEdit{color: green;}") 
            self.timeedit.setEnabled(False)        
            settings.setValue('Output/crontime', self.timeedit.time().toString())
            settings.sync()
            self.tray_icon.updateCrontime()
            self.tray_icon.startThread()
        else:     
            self.tray_icon.stopThread()
            self.timeedit.setStyleSheet("QTimeEdit{color: black;}") 
            self.timeedit.setEnabled(True) 

    def openDirectoryDialog(self):
           
        flags = QFileDialog.DontResolveSymlinks | QFileDialog.ShowDirsOnly
        d = directory = QFileDialog.getExistingDirectory(self,"Open Directory",os.getcwd(),flags)        
        self.FolderEdit.setText(d)
        settings.setValue('Output/folder', d)
        settings.sync()

        self.createReportFolder()

    def createReportFolder(self):

        configOutput = settings.value('Output/folder', "")
        
        self.dirDaily = configOutput+"/Daily"
        self.dirMonthly = configOutput+"/Monthly"
        self.dirYearly = configOutput+"/Yearly"

        Path(self.dirDaily).mkdir(parents=True, exist_ok=True)
        Path(self.dirMonthly).mkdir(parents=True, exist_ok=True)
        Path(self.dirYearly ).mkdir(parents=True, exist_ok=True)

    def testConnection(self):

        self.db.setHostName(str(self.pgHostEdit.text()))
        self.db.setDatabaseName(str(self.pgDatabaseEdit.text()))
        self.db.setPort(int(self.pgPortEdit.text()))
        self.db.setUserName(str(self.pgUsernameEdit.text()))
        self.db.setPassword(str(self.pgPasswordEdit.text()))
        ok = self.db.open()
        if not ok: 
            QMessageBox.information(self, 'Message', str(self.db.lastError().text()), QMessageBox.Ok)
        else: 
            ret = QMessageBox.information(self, 'Message', "Test connection succeeded.", QMessageBox.Ok)
            if ret == QMessageBox.Ok:
                # print('seve setting')
                settings.setValue('Postgresql/database', self.pgDatabaseEdit.text())
                settings.setValue('Postgresql/host', self.pgHostEdit.text())
                settings.setValue('Postgresql/port', int(self.pgPortEdit.text()))
                settings.setValue('Postgresql/username', self.pgUsernameEdit.text())
                settings.setValue('Postgresql/password', self.pgPasswordEdit.text())
                settings.sync()

                file = open('keytagid.log','w+')

                for i in range(len(keytagname)):
                    for j in range(1,len(keytagname[i])-1):
                        a_list = []
                        # print(keytagid[i][j])
                        query = QSqlQuery(self.db)
                        query.exec_('select "KeyTag" from "DataPointDetail" where "Name" = \''+keytagname[i][j]+'\'')
                        while query.next():
                            # print(query.value('KeyTag'))
                            a_list.append(query.value('KeyTag'))
                        keytagid[i][j] = a_list
                        file.write('Name: '+str(keytagname[i][j])+' | KeyTag: '+ ",".join(str(x) for x in a_list)+'\n')
                self.db.close()
                file.close()

    def createMonthlyReport(self):
   
        self.db4 = QSqlDatabase.addDatabase('QPSQL','MonthlyConnect')
        self.db4.setHostName(configHost)
        self.db4.setDatabaseName(configDatabase)
        self.db4.setPort(int(configPort))
        self.db4.setUserName(configUsername)
        self.db4.setPassword(configPassword)
        ok = self.db4.open()
        if not ok:
            print("Monthly Connection Error: " + str(self.db4.lastError().text()))

        smonth = self.combo.currentIndex()+1
        syear = self.combo2.currentText()

        if smonth == 12:
            sdate = date(int(syear), int(smonth), 1)  
            edate = date(int(syear)+1, 1, 1) 
        else: 
            sdate = date(int(syear), int(smonth), 1)  
            edate = date(int(syear), int(smonth)+1, 1) 

        delta = edate - sdate
        index = self.monthlyReports.currentIndex()
        excel_month = sdate.strftime("%Y%m")
        row = 12

        workbook = xlsxwriter.Workbook(self.dirMonthly+"/"+str(excel_month)+'_PEA_Monthly_report_'+keytagname[index][0]+'.xlsx')
        worksheet = workbook.add_worksheet()
        worksheet.set_paper(9)
        worksheet.set_landscape()
        worksheet.fit_to_pages(1, 0)
        
        border_format=workbook.add_format({'border':1})
        worksheet.conditional_format('A9:AG42',{'type':'blanks','format' : border_format} )

        worksheet.insert_image('A1', 'pea.png',{'x_scale': 1.25, 'y_scale': 1.25})
        cell_format_header = workbook.add_format({'align': 'center','valign': 'vcenter','bold': True, 'font_size': 14})
        
        worksheet.merge_range('O2:V2', "Monthly report", cell_format_header)
        worksheet.merge_range('O3:V3', configSite + " substation", cell_format_header)
        worksheet.merge_range('O4:V4', keytagname[index][11], cell_format_header)
        worksheet.merge_range('O5:V5', "Month: " + str(self.combo.currentText() + " " + self.combo2.currentText()), cell_format_header)         
        cell_format_header_data = workbook.add_format({'align': 'center','valign': 'vcenter','bold': True, 'font_size': 9,'border':1})                    
        # day
        worksheet.set_column('A:A', 9)           
        worksheet.set_column('B:H', 6)
        worksheet.set_column('I:K', 7)

        worksheet.merge_range('A9:K9', "DAY TIME PEAK", cell_format_header_data)
        worksheet.merge_range('A10:K10', "08:00:00 - 15:30:00", cell_format_header_data)
        
        worksheet.write('A11', 'Date', cell_format_header_data)
        worksheet.write('B11', 'Time', cell_format_header_data)
        worksheet.write('C11', 'kV(AB)', cell_format_header_data)
        worksheet.write('D11', 'kV(BC)', cell_format_header_data)
        worksheet.write('E11', 'kV(CA)', cell_format_header_data)
        worksheet.write('F11', 'IA', cell_format_header_data)
        worksheet.write('G11', 'IB', cell_format_header_data)
        worksheet.write('H11', 'IC', cell_format_header_data)
        worksheet.write('I11', 'MW', cell_format_header_data)
        worksheet.write('J11', 'Mvar', cell_format_header_data)
        worksheet.write('K11', '%PF', cell_format_header_data)
        # night
        worksheet.set_column('L:L', 9)           
        worksheet.set_column('M:S', 6)
        worksheet.set_column('T:V', 7)

        worksheet.merge_range('L9:V9', "NIGHT TIME PEAK", cell_format_header_data)
        worksheet.merge_range('L10:V10', "00:00:00 - 08:00:00, 15:30:00 - 23:59:59", cell_format_header_data)
        
        worksheet.write('L11', 'Date', cell_format_header_data)
        worksheet.write('M11', 'Time', cell_format_header_data)
        worksheet.write('N11', 'kV(AB)', cell_format_header_data)
        worksheet.write('O11', 'kV(BC)', cell_format_header_data)
        worksheet.write('P11', 'kV(CA)', cell_format_header_data)
        worksheet.write('Q11', 'IA', cell_format_header_data)
        worksheet.write('R11', 'IB', cell_format_header_data)
        worksheet.write('S11', 'IC', cell_format_header_data)
        worksheet.write('T11', 'MW', cell_format_header_data)
        worksheet.write('U11', 'Mvar', cell_format_header_data)
        worksheet.write('V11', '%PF', cell_format_header_data)
        # low        
        worksheet.set_column('W:W', 9)           
        worksheet.set_column('X:AD', 6)
        worksheet.set_column('AE:AG', 7)

        worksheet.merge_range('W9:AG9', "DAY & NIGHT LIGHT LOAD", cell_format_header_data)
        worksheet.merge_range('W10:AG10', "00:00:00 - 23:59:59", cell_format_header_data)
        
        worksheet.write('W11', 'Date', cell_format_header_data)
        worksheet.write('X11', 'Time', cell_format_header_data)
        worksheet.write('Y11', 'kV(AB)', cell_format_header_data)
        worksheet.write('Z11', 'kV(BC)', cell_format_header_data)
        worksheet.write('AA11', 'kV(CA)', cell_format_header_data)
        worksheet.write('AB11', 'IA', cell_format_header_data)
        worksheet.write('AC11', 'IB', cell_format_header_data)
        worksheet.write('AD11', 'IC', cell_format_header_data)
        worksheet.write('AE11', 'MW', cell_format_header_data)
        worksheet.write('AF11', 'Mvar', cell_format_header_data)
        worksheet.write('AG11', '%PF', cell_format_header_data)

        float_format = workbook.add_format({'num_format': '###,###,##0.00', 'font_size': 8,'border':1})
        float_format_datecell = workbook.add_format({'num_format': 'd/m/yyyy', 'font_size': 8,'border':1,'align': 'right'})
        float_format_timecell = workbook.add_format({'num_format': 'hh:mm:ss','font_size': 8,'border':1,'align': 'right'})
        
        if os.path.exists("monthly.log"):
            os.remove("monthly.log")

        for i in range(delta.days):
            
            day = sdate + timedelta(days=i)
           
            file = open('monthly.log','a+')
            file.write('On Date: '+str(day.strftime("%Y-%m-%d"))+" | Time: "+str(datetime.now().time())+'\n')
            file.close()

            report_date = day.strftime("%Y-%m-%d")
            yesterday = (day - timedelta(1)).strftime('%Y-%m-%d')

            print(report_date)
            print("yesterday->",yesterday)

            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # current_datetime = datetime.now()
            print(current_datetime)
            
            today = date.today()

            worksheet.write('A'+ str(row), day,float_format_datecell)   
            worksheet.write('L'+ str(row), day,float_format_datecell)   
            worksheet.write('W'+ str(row), day,float_format_datecell)  
            
            query = QSqlQuery(self.db4)

            query_month_cmd = 'with ' \
                    'analog_transition_kvab as (' \
                    '    (SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC) ' \
                    '    UNION' \
                    '    (select (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time, NULL,NULL,NULL)' \
                    '),' \
                    'analog_transition_kvbc as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][2])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_kvca as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][3])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ia as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ib as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ic as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_mw as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_mvar as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_pctpf as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_kvab_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ia_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ib_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ic_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_mw_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_mvar_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_pctpf_down as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'first_records as(' \
                    'select' \
                    '    (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVAB,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][2])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVBC,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][3])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVCA,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IA,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IB,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IC,' \
                    '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                    '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MW,' \
                    '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                    '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MVar,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][9])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as pctPF' \
                    '),' \
                    'join_records as (' \
                    '    select kvab.start_time,' \
                    '    COALESCE(f_record.kVAB,kvab."Value") as kVAB,' \
                    '    COALESCE(f_record.kVBC,kvbc."Value") as kVBC,' \
                    '    COALESCE(f_record.kVCA,kvca."Value") as kVCA,' \
                    '    COALESCE(f_record.IA,ia."Value") as IA,' \
                    '    COALESCE(f_record.IB,ib."Value") as IB,' \
                    '    COALESCE(f_record.IC,ic."Value") as IC,' \
                    '    COALESCE(f_record.MW,(CASE WHEN ABS(mw."Value") <= 10000 THEN mw."Value"/100 WHEN ABS(mw."Value") > 10000 THEN mw."Value"/1000000 ELSE mw."Value" END)) as MW,' \
                    '    COALESCE(f_record.MVar,(CASE WHEN ABS(mvar."Value") <= 10000 THEN mvar."Value"/100 WHEN ABS(mvar."Value") > 10000 THEN mvar."Value"/1000000 ELSE mvar."Value" END)) as MVar,' \
                    '    COALESCE((CASE WHEN pctpf."Value" > 100 THEN pctpf."Value"/100 WHEN pctpf."Value" < -100 THEN pctpf."Value"/100 ELSE pctpf."Value" END),' \
                    '    (CASE WHEN f_record.pctPF > 100 THEN f_record.pctPF/100 WHEN f_record.pctPF < -100 THEN f_record.pctPF/100 ELSE f_record.pctPF END))as pctPF,' \
                    '    kvab_down."Value" as kVAB_down,' \
                    '    kvab_down."Timestamp" as kVAB_downtime,' \
                    '    kvab_down."start_time" as kVAB_downtime_start_time,' \
                    '    ia_down."Value" as IA_down,' \
                    '    ia_down."Timestamp" as IA_downtime,' \
                    '    ia_down."start_time" as IA_downtime_start_time,' \
                    '    ib_down."Value" as IB_down,' \
                    '    ib_down."Timestamp" as IB_downtime,' \
                    '    ib_down."start_time" as IB_downtime_start_time,' \
                    '    ic_down."Value" as IC_down,' \
                    '    ic_down."Timestamp" as IC_downtime,' \
                    '    ic_down."start_time" as IC_downtime_start_time,' \
                    '    mw_down."Value" as MW_down,' \
                    '    mw_down."Timestamp" as MW_downtime,' \
                    '    mw_down."start_time" as MW_downtime_start_time,' \
                    '    mvar_down."Value" as MVar_down,' \
                    '    mvar_down."Timestamp" as MVar_downtime,' \
                    '    mvar_down."start_time" as MVar_downtime_start_time,' \
                    '    pctpf_down."Value" as pctPF_down,' \
                    '    pctpf_down."Timestamp" as pctPF_downtime,' \
                    '    pctpf_down."start_time" as pctPF_downtime_start_time,' \
                    '    kvab."Timestamp" as kVAB_firstseentime ' \
                    '    from analog_transition_kvab kvab' \
                    '    LEFT JOIN analog_transition_kvbc kvbc ON kvab.start_time = kvbc.start_time' \
                    '    LEFT JOIN analog_transition_kvca kvca ON kvab.start_time = kvca.start_time ' \
                    '    LEFT JOIN analog_transition_ia ia ON kvab.start_time = ia.start_time' \
                    '    LEFT JOIN analog_transition_ib ib ON kvab.start_time = ib.start_time ' \
                    '    LEFT JOIN analog_transition_ic ic ON kvab.start_time = ic.start_time ' \
                    '    LEFT JOIN analog_transition_mw mw ON kvab.start_time = mw.start_time ' \
                    '    LEFT JOIN analog_transition_mvar mvar ON kvab.start_time = mvar.start_time ' \
                    '    LEFT JOIN analog_transition_pctpf pctpf ON kvab.start_time = pctpf.start_time ' \
                    '    LEFT JOIN analog_transition_kvab_down kvab_down ON kvab.start_time = kvab_down.start_time' \
                    '    LEFT JOIN analog_transition_ia_down ia_down ON kvab.start_time = ia_down.start_time' \
                    '    LEFT JOIN analog_transition_ib_down ib_down ON kvab.start_time = ib_down.start_time' \
                    '    LEFT JOIN analog_transition_ic_down ic_down ON kvab.start_time = ic_down.start_time' \
                    '    LEFT JOIN analog_transition_mw_down mw_down ON kvab.start_time = mw_down.start_time' \
                    '    LEFT JOIN analog_transition_mvar_down mvar_down ON kvab.start_time = mvar_down.start_time' \
                    '    LEFT JOIN analog_transition_pctpf_down pctpf_down ON kvab.start_time = pctpf_down.start_time' \
                    '    LEFT JOIN first_records f_record ON kvab.start_time = f_record.start_time' \
                    '    Order by kvab.start_time ASC' \
                    '),' \
                    'prior_ia as (' \
                    '    select start_time,' \
                    '    (select js_sub.IA from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IA is not null ORDER BY js_sub.start_time DESC limit 1) as priorIA' \
                    '    from join_records js where IA IS NULL' \
                    '),' \
                    'prior_ib as (' \
                    '    select start_time,' \
                    '    (select js_sub.IB from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IB is not null ORDER BY js_sub.start_time DESC limit 1) as priorIB' \
                    '    from join_records js where IB IS NULL' \
                    '),' \
                    'prior_ic as (' \
                    '    select start_time,' \
                    '    (select js_sub.IC from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IC is not null ORDER BY js_sub.start_time DESC limit 1) as priorIC' \
                    '    from join_records js where IC IS NULL' \
                    '),' \
                    'prior_mw as (' \
                    '    select start_time,' \
                    '    (select js_sub.MW from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.MW is not null ORDER BY js_sub.start_time DESC limit 1) as priorMW' \
                    '    from join_records js where MW IS NULL' \
                    '),' \
                    'prior_mvar as (' \
                    '    select start_time,' \
                    '    (select js_sub.MVar from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.MVar is not null ORDER BY js_sub.start_time DESC limit 1) as priorMVar' \
                    '    from join_records js where MVar IS NULL' \
                    '),' \
                    'prior_pctpf as (' \
                    '    select start_time,' \
                    '    (select js_sub.pctPF from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.pctPF is not null ORDER BY js_sub.start_time DESC limit 1) as priorpctPF' \
                    '    from join_records js where pctPF IS NULL' \
                    '),' \
                    'result_records as (' \
                    '    select js.start_time,' \
                    '    COALESCE(js.kVAB_down, js.kVAB) as kVAB,' \
                    '    COALESCE(js.kVAB_down, js.kVBC) as kVBC,' \
                    '    COALESCE(js.kVAB_down, js.kVCA) as kVCA,' \
                    '    COALESCE(js.IA_down, COALESCE(js.IA, p_ia.priorIA)) as IA,' \
                    '    COALESCE(js.IB_down, COALESCE(js.IB, p_ib.priorIB)) as IB,' \
                    '    COALESCE(js.IC_down, COALESCE(js.IC, p_ic.priorIC)) as IC,' \
                    '    COALESCE(js.MW_down, COALESCE(js.MW, p_mw.priorMW)) as MW,' \
                    '    COALESCE(js.MVar_down, COALESCE(js.MVar, p_mvar.priorMVar)) as MVar,' \
                    '    COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF)) as pctPF' \
                    '    from join_records js' \
                    '    LEFT JOIN prior_ia p_ia ON js.start_time = p_ia.start_time' \
                    '    LEFT JOIN prior_ib p_ib ON js.start_time = p_ib.start_time' \
                    '    LEFT JOIN prior_ic p_ic ON js.start_time = p_ic.start_time' \
                    '    LEFT JOIN prior_mw p_mw ON js.start_time = p_mw.start_time' \
                    '    LEFT JOIN prior_mvar p_mvar ON js.start_time = p_mvar.start_time' \
                    '    LEFT JOIN prior_pctpf p_prior_pctpf ON js.start_time = p_prior_pctpf.start_time' \
                    '),' \
                    'record_daypeak as ( ' \
                    '   select start_time as dayStartTime, kVAB as daykVAB,kVBC as daykVBC ,kVCA as daykVCA, ' \
                    '   IA as dayIA ,IB as dayIB,IC as dayIC,pctPF as daypctPF, MW as dayMW , MVar as dayMVar ' \
                    '   from result_records where start_time >= \''+report_date+' 08:00:00\' and start_time <= \''+report_date+' 15:30:00\' ' \
                    '   and start_time < \''+current_datetime+'\' order by ABS(MW) DESC Limit 1 ' \
                    '),' \
                    'record_nightpeak as ( ' \
                    '   select start_time as nightStartTime, kVAB as nightkVAB,kVBC as nightkVBC ,kVCA as nightkVCA, ' \
                    '   IA as nightIA ,IB as nightIB,IC as nightIC,pctPF as nightpctPF, MW as nightMW, MVar as nightMVar  ' \
                    '   from result_records where ((start_time >= \''+report_date+' 00:00:00\' and start_time <= \''+report_date+' 07:30:00\') or  ' \
                    '   (start_time >= \''+report_date+' 16:00:00\' and start_time <= \''+report_date+' 23:00:00\')) ' \
                    '   and start_time < \''+current_datetime+'\' order by ABS(MW) DESC Limit 1 ' \
                    '),' \
                    'record_daynightlight as ( ' \
                    '   select start_time as lightStartTime, kVAB as lightkVAB,kVBC as lightkVBC ,kVCA as lightkVCA, ' \
                    '   IA as lightIA ,IB as lightIB,IC as lightIC,pctPF as lightpctPF, MW as lightMW, MVar as lightMVar  ' \
                    '   from result_records where MW != \'0.00\' and start_time >= \''+report_date+' 00:00:00\' and start_time <= \''+report_date+' 23:59:59\' ' \
                    '   and start_time < \''+current_datetime+'\' order by ABS(MW) ASC Limit 1 ' \
                    ')' \
                    ' ' \
                    'select *' \
                    'from record_daypeak dp FULL join record_nightpeak np ON True FULL join record_daynightlight al ON True limit 1'

            query.exec_(query_month_cmd)

            while query.next():
                # day
                if len(query.value('dayStartTime').toString("HH:mm:ss")) == 8:
                    worksheet.write('B'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('dayStartTime'), "hh:mm:ss"),float_format_timecell) 
                if isinstance(query.value('daykVAB'), float):
                    worksheet.write_number('C'+ str(row), query.value('daykVAB'),float_format)
                if isinstance(query.value('daykVBC'), float):
                    worksheet.write_number('D'+ str(row), query.value('daykVBC'),float_format)
                if isinstance(query.value('daykVCA'), float):
                    worksheet.write_number('E'+ str(row), query.value('daykVCA'),float_format)
                if isinstance(query.value('dayIA'), float):
                    worksheet.write_number('F'+ str(row), query.value('dayIA'),float_format)
                if isinstance(query.value('dayIB'), float):
                    worksheet.write_number('G'+ str(row), query.value('dayIB'),float_format)
                if isinstance(query.value('dayIC'), float):
                    worksheet.write_number('H'+ str(row), query.value('dayIC'),float_format)
                if isinstance(query.value('dayMW'), float) or isinstance(query.value('dayMW'), int):
                    worksheet.write_number('I'+ str(row), query.value('dayMW'),float_format)
                if isinstance(query.value('dayMVar'), float) or isinstance(query.value('dayMVar'), int):                    
                    worksheet.write_number('J'+ str(row), query.value('dayMVar'),float_format)
                if isinstance(query.value('daypctPF'), float) :
                    worksheet.write_number('K'+ str(row), query.value('daypctPF'),float_format)
                #night
                if len(query.value('nightStartTime').toString("HH:mm:ss")) == 8:
                    worksheet.write('M'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('nightStartTime'), "hh:mm:ss"),float_format_timecell) 
                if isinstance(query.value('nightkVAB'), float):
                    worksheet.write_number('N'+ str(row), query.value('nightkVAB'),float_format)
                if isinstance(query.value('nightkVBC'), float) :
                    worksheet.write_number('O'+ str(row), query.value('nightkVBC'),float_format)
                if isinstance(query.value('nightkVCA'), float):
                    worksheet.write_number('P'+ str(row), query.value('nightkVCA'),float_format)
                if isinstance(query.value('nightIA'), float):
                    worksheet.write_number('Q'+ str(row), query.value('nightIA'),float_format)
                if isinstance(query.value('nightIB'), float):
                    worksheet.write_number('R'+ str(row), query.value('nightIB'),float_format)
                if isinstance(query.value('nightIC'), float):
                    worksheet.write_number('S'+ str(row), query.value('nightIC'),float_format)
                if isinstance(query.value('nightMW'), float) or isinstance(query.value('nightMW'), int):
                    worksheet.write_number('T'+ str(row), query.value('nightMW'),float_format)
                    # worksheet.write_number('U'+ str(row), query.value('nightMVar')),float_format)
                if isinstance(query.value('nightMVar'), float) or isinstance(query.value('nightMVar'), int):
                    worksheet.write_number('U'+ str(row), query.value('nightMVar'),float_format)
                if isinstance(query.value('nightpctPF'), float) :
                    worksheet.write_number('V'+ str(row), query.value('nightpctPF'),float_format)
                #light
                if len(query.value('lightStartTime').toString("HH:mm:ss")) == 8:
                    worksheet.write('X'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('lightStartTime'), "hh:mm:ss"),float_format_timecell)  
                if isinstance(query.value('lightkVAB'), float):
                    worksheet.write_number('Y'+ str(row), float(query.value('lightkVAB')),float_format)
                if isinstance(query.value('lightkVBC'), float) :
                    worksheet.write_number('Z'+ str(row), float(query.value('lightkVBC')),float_format)
                if isinstance(query.value('lightkVCA'), float):
                    worksheet.write_number('AA'+ str(row), float(query.value('lightkVCA')),float_format)
                if isinstance(query.value('lightIA'), float):
                    worksheet.write_number('AB'+ str(row), float(query.value('lightIA')),float_format)
                if isinstance(query.value('lightIB'), float):
                    worksheet.write_number('AC'+ str(row), float(query.value('lightIB')),float_format)
                if isinstance(query.value('lightIC'), float):
                    worksheet.write_number('AD'+ str(row), float(query.value('lightIC')),float_format)
                if isinstance(query.value('lightMW'), float) or isinstance(query.value('lightMW'), int):
                    worksheet.write_number('AE'+ str(row), float(query.value('lightMW')),float_format)
                if isinstance(query.value('lightMVar'), float) or isinstance(query.value('lightMVar'), int):
                    worksheet.write_number('AF'+ str(row), float(query.value('lightMVar')),float_format)
                if isinstance(query.value('lightpctPF'), float) :
                    worksheet.write_number('AG'+ str(row), float(query.value('lightpctPF')),float_format)

                if row >= 39:
                    worksheet.conditional_format('A39:AG'+str(row)+'',{'type':'blanks','format' : border_format} )
            row += 1

        workbook.close()
        self.db4.close()
        os.system("start EXCEL.EXE " + self.dirMonthly+"/"+str(excel_month)+'_PEA_Monthly_report_'+keytagname[index][0]+'.xlsx')

    def createYearlyReport(self):

        self.db5 = QSqlDatabase.addDatabase('QPSQL','YearlyConnect')
        self.db5.setHostName(configHost)
        self.db5.setDatabaseName(configDatabase)
        self.db5.setPort(int(configPort))
        self.db5.setUserName(configUsername)
        self.db5.setPassword(configPassword)
        ok = self.db5.open()
        if not ok:
            print("Yearly Connection Error: " + str(self.db5.lastError().text()))

        syear = self.combo3.currentText()        
        index = self.yearlyReports.currentIndex()
        row = 12

        workbook = xlsxwriter.Workbook(self.dirYearly+"/"+str(syear)+'_PEA_Yearly_report_'+keytagname[index][0]+'.xlsx')
        worksheet = workbook.add_worksheet()
        worksheet.set_paper(9)
        worksheet.set_landscape()
        worksheet.fit_to_pages(1, 0)
        
        border_format=workbook.add_format({'border':1})
        worksheet.conditional_format('A9:V23',{'type':'blanks','format' : border_format} )

        worksheet.insert_image('A1', 'pea.png',{'x_scale': 1.25, 'y_scale': 1.25})

        cell_format_header = workbook.add_format({'align': 'center',
                                                'valign': 'vcenter',
                                                'bold': True, 'font_size': 14})

        worksheet.merge_range('H2:O2', "Yearly report", cell_format_header)
        worksheet.merge_range('H3:O3', configSite + " substation", cell_format_header)
        worksheet.merge_range('H4:O4', keytagname[index][11], cell_format_header)
        worksheet.merge_range('H5:O5', "Year: " + str(" " + self.combo2.currentText()), cell_format_header)         

        cell_format_header_data = workbook.add_format({'align': 'center',
                'valign': 'vcenter',
                'bold': True, 'font_size': 9,'border':1})                    
        # day
        worksheet.set_column('A:A', 9)           
        worksheet.set_column('B:H', 6)
        worksheet.set_column('I:K', 7)
        worksheet.merge_range('A9:K9', "PEAK LOAD", cell_format_header_data)
        worksheet.merge_range('A10:K10', "00:00:00 - 23:59:59", cell_format_header_data)
        
        worksheet.write('A11', 'Date', cell_format_header_data)
        worksheet.write('B11', 'Time', cell_format_header_data)
        worksheet.write('C11', 'kV(AB)', cell_format_header_data)
        worksheet.write('D11', 'kV(BC)', cell_format_header_data)
        worksheet.write('E11', 'kV(CA)', cell_format_header_data)
        worksheet.write('F11', 'IA', cell_format_header_data)
        worksheet.write('G11', 'IB', cell_format_header_data)
        worksheet.write('H11', 'IC', cell_format_header_data)
        worksheet.write('I11', 'MW', cell_format_header_data)
        worksheet.write('J11', 'Mvar', cell_format_header_data)
        worksheet.write('K11', '%PF', cell_format_header_data)
        # night
        worksheet.set_column('L:L', 9)           
        worksheet.set_column('M:S', 6)
        worksheet.set_column('T:V', 7)
        worksheet.merge_range('L9:V9', "LIGHT LOAD", cell_format_header_data)
        worksheet.merge_range('L10:V10', "00:00:00 - 23:59:59", cell_format_header_data)
        
        worksheet.write('L11', 'Date', cell_format_header_data)
        worksheet.write('M11', 'Time', cell_format_header_data)
        worksheet.write('N11', 'kV(AB)', cell_format_header_data)
        worksheet.write('O11', 'kV(BC)', cell_format_header_data)
        worksheet.write('P11', 'kV(CA)', cell_format_header_data)
        worksheet.write('Q11', 'IA', cell_format_header_data)
        worksheet.write('R11', 'IB', cell_format_header_data)
        worksheet.write('S11', 'IC', cell_format_header_data)
        worksheet.write('T11', 'MW', cell_format_header_data)
        worksheet.write('U11', 'Mvar', cell_format_header_data)
        worksheet.write('V11', '%PF', cell_format_header_data)
        
        float_format = workbook.add_format({'num_format': '###,###,##0.00', 'font_size': 8,'border':1})
        float_format_datecell = workbook.add_format({'num_format': 'd/m/yyyy', 'font_size': 8,'border':1,'align': 'right'})
        float_format_timecell = workbook.add_format({'num_format': 'hh:mm:ss','font_size': 8,'border':1,'align': 'right'})

        if os.path.exists("yearly.log"):
            os.remove("yearly.log")

        for i in range(1,13):
                        
            if(i == 12):
                sdate = date(int(syear), int(i), 1)
                edate = date(int(syear)+1, 1, 1)
            else:
                sdate = date(int(syear), int(i), 1)
                edate = date(int(syear), int(i)+1, 1)

            hdate = sdate.replace(day=7)
            hdate2 = sdate.replace(day=14)   
            hdate3 = sdate.replace(day=21)

            start_report_date = sdate.strftime("%Y-%m-%d")
            end_report_date = edate.strftime("%Y-%m-%d")
            haft_report_date = hdate.strftime("%Y-%m-%d")
            haft_report_date2 = hdate2.strftime("%Y-%m-%d")
            haft_report_date3 = hdate3.strftime("%Y-%m-%d")

            range_date = [start_report_date,haft_report_date,haft_report_date2,haft_report_date3,end_report_date]
            prior_dayMW  = []
            prior_lightMW  = []   

            for x in range(len(range_date)-1): 
                file = open('yearly.log','a+')
                print(range_date[x],range_date[x+1])
                file.write('Process Date: '+str(range_date[x])+"-"+range_date[x+1]+" | Time: "+str(datetime.now().time())+'\n')
                
                date1 = datetime.strptime(range_date[x], "%Y-%m-%d")
                yesterday = (date1 - timedelta(1)).strftime('%Y-%m-%d')

                date2 = datetime.strptime(range_date[x+1], "%Y-%m-%d")
                report_date = (date2 - timedelta(1)).strftime('%Y-%m-%d')
                print(yesterday,"",report_date)

                # --------------------------------------------------
                query = QSqlQuery(self.db5)

                # query_year_cmd = 'with ' \
                query_year_cmd = 'with ' \
                        'analog_transition_kvab as (' \
                        '    (SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC) ' \
                        '    UNION' \
                        '    (select (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time, NULL,NULL,NULL)' \
                        '),' \
                        'analog_transition_kvbc as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][2])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_kvca as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][3])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_ia as (' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_ib as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_ic as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_mw as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_mvar as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_pctpf as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" is not null' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                        '),' \
                        'analog_transition_kvab_down as (' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" = \'0.00\'' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                        '),' \
                        'analog_transition_ia_down as (' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" = \'0.00\'' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                        '),' \
                        'analog_transition_ib_down as (' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" = \'0.00\'' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                        '),' \
                        'analog_transition_ic_down as (' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" = \'0.00\'' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                        '),' \
                        'analog_transition_mw_down as (' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" = \'0.00\'' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                        '),' \
                        'analog_transition_mvar_down as (' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" = \'0.00\'' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                        '),' \
                        'analog_transition_pctpf_down as ( ' \
                        '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                        '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                        '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                        '    from "AnalogTransition" ant ' \
                        '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')' \
                        '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                        '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                        '    and ant."Value" = \'0.00\'' \
                        '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                        '),' \
                        'first_records as(' \
                        'select' \
                        '    (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time,' \
                        '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVAB,' \
                        '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][2])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVBC,' \
                        '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][3])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVCA,' \
                        '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IA,' \
                        '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IB,' \
                        '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IC,' \
                        '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                        '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MW,' \
                        '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                        '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MVar,' \
                        '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][9])+')' \
                        '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                        '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as pctPF' \
                        '),' \
                        'join_records as (' \
                        '    select kvab.start_time,' \
                        '    COALESCE(f_record.kVAB,kvab."Value") as kVAB,' \
                        '    COALESCE(f_record.kVBC,kvbc."Value") as kVBC,' \
                        '    COALESCE(f_record.kVCA,kvca."Value") as kVCA,' \
                        '    COALESCE(f_record.IA,ia."Value") as IA,' \
                        '    COALESCE(f_record.IB,ib."Value") as IB,' \
                        '    COALESCE(f_record.IC,ic."Value") as IC,' \
                        '    COALESCE(f_record.MW,(CASE WHEN ABS(mw."Value") <= 10000 THEN mw."Value"/100 WHEN ABS(mw."Value") > 10000 THEN mw."Value"/1000000 ELSE mw."Value" END)) as MW,' \
                        '    COALESCE(f_record.MVar,(CASE WHEN ABS(mvar."Value") <= 10000 THEN mvar."Value"/100 WHEN ABS(mvar."Value") > 10000 THEN mvar."Value"/1000000 ELSE mvar."Value" END)) as MVar,' \
                        '    COALESCE((CASE WHEN pctpf."Value" > 100 THEN pctpf."Value"/100 WHEN pctpf."Value" < -100 THEN pctpf."Value"/100 ELSE pctpf."Value" END),' \
                        '    (CASE WHEN f_record.pctPF > 100 THEN f_record.pctPF/100 WHEN f_record.pctPF < -100 THEN f_record.pctPF/100 ELSE f_record.pctPF END))as pctPF,' \
                        '    kvab_down."Value" as kVAB_down,' \
                        '    kvab_down."Timestamp" as kVAB_downtime,' \
                        '    kvab_down."start_time" as kVAB_downtime_start_time,' \
                        '    ia_down."Value" as IA_down,' \
                        '    ia_down."Timestamp" as IA_downtime,' \
                        '    ia_down."start_time" as IA_downtime_start_time,' \
                        '    ib_down."Value" as IB_down,' \
                        '    ib_down."Timestamp" as IB_downtime,' \
                        '    ib_down."start_time" as IB_downtime_start_time,' \
                        '    ic_down."Value" as IC_down,' \
                        '    ic_down."Timestamp" as IC_downtime,' \
                        '    ic_down."start_time" as IC_downtime_start_time,' \
                        '    mw_down."Value" as MW_down,' \
                        '    mw_down."Timestamp" as MW_downtime,' \
                        '    mw_down."start_time" as MW_downtime_start_time,' \
                        '    mvar_down."Value" as MVar_down,' \
                        '    mvar_down."Timestamp" as MVar_downtime,' \
                        '    mvar_down."start_time" as MVar_downtime_start_time,' \
                        '    pctpf_down."Value" as pctPF_down,' \
                        '    pctpf_down."Timestamp" as pctPF_downtime,' \
                        '    pctpf_down."start_time" as pctPF_downtime_start_time,' \
                        '    kvab."Timestamp" as kVAB_firstseentime ' \
                        '    from analog_transition_kvab kvab' \
                        '    LEFT JOIN analog_transition_kvbc kvbc ON kvab.start_time = kvbc.start_time' \
                        '    LEFT JOIN analog_transition_kvca kvca ON kvab.start_time = kvca.start_time ' \
                        '    LEFT JOIN analog_transition_ia ia ON kvab.start_time = ia.start_time' \
                        '    LEFT JOIN analog_transition_ib ib ON kvab.start_time = ib.start_time ' \
                        '    LEFT JOIN analog_transition_ic ic ON kvab.start_time = ic.start_time ' \
                        '    LEFT JOIN analog_transition_mw mw ON kvab.start_time = mw.start_time ' \
                        '    LEFT JOIN analog_transition_mvar mvar ON kvab.start_time = mvar.start_time ' \
                        '    LEFT JOIN analog_transition_pctpf pctpf ON kvab.start_time = pctpf.start_time ' \
                        '    LEFT JOIN analog_transition_kvab_down kvab_down ON kvab.start_time = kvab_down.start_time' \
                        '    LEFT JOIN analog_transition_ia_down ia_down ON kvab.start_time = ia_down.start_time' \
                        '    LEFT JOIN analog_transition_ib_down ib_down ON kvab.start_time = ib_down.start_time' \
                        '    LEFT JOIN analog_transition_ic_down ic_down ON kvab.start_time = ic_down.start_time' \
                        '    LEFT JOIN analog_transition_mw_down mw_down ON kvab.start_time = mw_down.start_time' \
                        '    LEFT JOIN analog_transition_mvar_down mvar_down ON kvab.start_time = mvar_down.start_time' \
                        '    LEFT JOIN analog_transition_pctpf_down pctpf_down ON kvab.start_time = pctpf_down.start_time' \
                        '    LEFT JOIN first_records f_record ON kvab.start_time = f_record.start_time' \
                        '    Order by kvab.start_time ASC' \
                        '),' \
                        'prior_ia as (' \
                        '    select start_time,' \
                        '    (select js_sub.IA from join_records js_sub where' \
                        '    js_sub.start_time < js.start_time and js_sub.IA is not null ORDER BY js_sub.start_time DESC limit 1) as priorIA' \
                        '    from join_records js where IA IS NULL' \
                        '),' \
                        'prior_ib as (' \
                        '    select start_time,' \
                        '    (select js_sub.IB from join_records js_sub where' \
                        '    js_sub.start_time < js.start_time and js_sub.IB is not null ORDER BY js_sub.start_time DESC limit 1) as priorIB' \
                        '    from join_records js where IB IS NULL' \
                        '),' \
                        'prior_ic as (' \
                        '    select start_time,' \
                        '    (select js_sub.IC from join_records js_sub where' \
                        '    js_sub.start_time < js.start_time and js_sub.IC is not null ORDER BY js_sub.start_time DESC limit 1) as priorIC' \
                        '    from join_records js where IC IS NULL' \
                        '),' \
                        'prior_mw as (' \
                        '    select start_time,' \
                        '    (select js_sub.MW from join_records js_sub where' \
                        '    js_sub.start_time < js.start_time and js_sub.MW is not null ORDER BY js_sub.start_time DESC limit 1) as priorMW' \
                        '    from join_records js where MW IS NULL' \
                        '),' \
                        'prior_mvar as (' \
                        '    select start_time,' \
                        '    (select js_sub.MVar from join_records js_sub where' \
                        '    js_sub.start_time < js.start_time and js_sub.MVar is not null ORDER BY js_sub.start_time DESC limit 1) as priorMVar' \
                        '    from join_records js where MVar IS NULL' \
                        '),' \
                        'prior_pctpf as (' \
                        '    select start_time,' \
                        '    (select js_sub.pctPF from join_records js_sub where' \
                        '    js_sub.start_time < js.start_time and js_sub.pctPF is not null ORDER BY js_sub.start_time DESC limit 1) as priorpctPF' \
                        '    from join_records js where pctPF IS NULL' \
                        '),' \
                        'result_records as (' \
                        '    select js.start_time,' \
                        '    COALESCE(js.kVAB_down, js.kVAB) as kVAB,' \
                        '    COALESCE(js.kVAB_down, js.kVBC) as kVBC,' \
                        '    COALESCE(js.kVAB_down, js.kVCA) as kVCA,' \
                        '    COALESCE(js.IA_down, COALESCE(js.IA, p_ia.priorIA)) as IA,' \
                        '    COALESCE(js.IB_down, COALESCE(js.IB, p_ib.priorIB)) as IB,' \
                        '    COALESCE(js.IC_down, COALESCE(js.IC, p_ic.priorIC)) as IC,' \
                        '    COALESCE(js.MW_down, COALESCE(js.MW, p_mw.priorMW)) as MW,' \
                        '    COALESCE(js.MVar_down, COALESCE(js.MVar, p_mvar.priorMVar)) as MVar,' \
                        '    COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF)) as pctPF' \
                        '    from join_records js' \
                        '    LEFT JOIN prior_ia p_ia ON js.start_time = p_ia.start_time' \
                        '    LEFT JOIN prior_ib p_ib ON js.start_time = p_ib.start_time' \
                        '    LEFT JOIN prior_ic p_ic ON js.start_time = p_ic.start_time' \
                        '    LEFT JOIN prior_mw p_mw ON js.start_time = p_mw.start_time' \
                        '    LEFT JOIN prior_mvar p_mvar ON js.start_time = p_mvar.start_time' \
                        '    LEFT JOIN prior_pctpf p_prior_pctpf ON js.start_time = p_prior_pctpf.start_time' \
                        '),' \
                        'record_daypeak as ( ' \
                        '   select start_time as day_starttime,kVAB as daykVAB, kVBC as daykVBC, kVCA as daykVCA, IA as dayIA,IB as dayIB,IC as dayIC,MW as dayMW,MVar as dayMVar,pctPF as daypctPF' \
                        '   from result_records where start_time > \''+yesterday+' 23:00:00\' and start_time <= \''+report_date+' 23:59:59.999\' ' \
                        '   order by ABS(MW) DESC limit 1' \
                        '),' \
                        'record_daylight as ( ' \
                        '   select start_time as light_starttime,kVAB as lightkVAB, kVBC as lightkVBC, kVCA as lightkVCA, IA as lightIA,IB as lightIB,IC as lightIC,MW as lightMW,MVar as lightMVar,pctPF as lightpctPF' \
                        '   from result_records where MW != \'0.00\' and start_time > \''+yesterday+' 23:00:00\' and start_time <= \''+report_date+' 23:59:59.999\'  ' \
                        '   order by ABS(MW) ASC limit 1' \
                        ')' \
                        ' ' \
                        'select *' \
                        'from record_daypeak dp FULL join record_daylight dl ON True limit 1'


                # print("--------------------Year--------------------------")
                # print(query_year_cmd)
                # print("--------------------------------------------------")

                query.exec_(query_year_cmd)

                while query.next():

                    prior_dayMW.append(abs(query.value('dayMW')))
                    prior_lightMW.append(abs(query.value('lightMW')))

                    file.write(' dayMW = '+str(query.value('dayMW'))+" | LightMW = "+str(query.value('lightMW'))+'\n')
                    # print(query.value('day_starttime'),query.value('dayMW'))
                    # print(query.value('light_starttime'),query.value('lightMW'))
                    # print("MAX->",max(prior_dayMW))
                    # print("MIN->",min(prior_lightMW))

                    if(abs(query.value('dayMW')) >= max(prior_dayMW)):
                        if len(query.value('day_starttime').toString("HH:mm:ss")) == 8:
                            worksheet.write('A'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('day_starttime'), "d/M/yyyy"),float_format_datecell) 
                            worksheet.write('B'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('day_starttime'), "hh:mm:ss"),float_format_timecell) 
                        if isinstance(query.value('daykVAB'), float):
                            worksheet.write_number('C'+ str(row), query.value('daykVAB'),float_format)
                        if isinstance(query.value('daykVBC'), float):
                            worksheet.write_number('D'+ str(row), query.value('daykVBC'),float_format)
                        if isinstance(query.value('daykVCA'), float):
                            worksheet.write_number('E'+ str(row), query.value('daykVCA'),float_format)
                        if isinstance(query.value('dayIA'), float):
                            worksheet.write_number('F'+ str(row), query.value('dayIA'),float_format)
                        if isinstance(query.value('dayIB'), float):
                            worksheet.write_number('G'+ str(row), query.value('dayIB'),float_format)
                        if isinstance(query.value('dayIC'), float):
                            worksheet.write_number('H'+ str(row), query.value('dayIC'),float_format)
                        if isinstance(query.value('dayMW'), float) or isinstance(query.value('dayMW'), int):
                            worksheet.write_number('I'+ str(row), query.value('dayMW'),float_format)
                        if isinstance(query.value('dayMVar'), float) or isinstance(query.value('dayMVar'), int):                    
                            worksheet.write_number('J'+ str(row), query.value('dayMVar'),float_format)
                        if isinstance(query.value('daypctPF'), float) :
                            worksheet.write_number('K'+ str(row), query.value('daypctPF'),float_format)

                    if(abs(query.value('lightMW')) <= max(prior_lightMW)):
                        if len(query.value('light_starttime').toString("HH:mm:ss")) == 8:
                            worksheet.write('L'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('light_starttime'), "d/M/yyyy"),float_format_datecell) 
                            worksheet.write('M'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('light_starttime'), "hh:mm:ss"),float_format_timecell) 
                        if isinstance(query.value('lightkVAB'), float):
                            worksheet.write_number('N'+ str(row), float(query.value('lightkVAB')),float_format)
                        if isinstance(query.value('lightkVBC'), float) :
                            worksheet.write_number('O'+ str(row), float(query.value('lightkVBC')),float_format)
                        if isinstance(query.value('lightkVCA'), float):
                            worksheet.write_number('P'+ str(row), float(query.value('lightkVCA')),float_format)
                        if isinstance(query.value('lightIA'), float):
                            worksheet.write_number('Q'+ str(row), float(query.value('lightIA')),float_format)
                        if isinstance(query.value('lightIB'), float):
                            worksheet.write_number('R'+ str(row), float(query.value('lightIB')),float_format)
                        if isinstance(query.value('lightIC'), float):
                            worksheet.write_number('S'+ str(row), float(query.value('lightIC')),float_format)
                        if isinstance(query.value('lightMW'), float) or isinstance(query.value('lightMW'), int):
                            worksheet.write_number('T'+ str(row), float(query.value('lightMW')),float_format)
                        if isinstance(query.value('lightMVar'), float) or isinstance(query.value('lightMVar'), int):
                            worksheet.write_number('U'+ str(row), float(query.value('lightMVar')),float_format)
                        if isinstance(query.value('lightpctPF'), float) :
                            worksheet.write_number('V'+ str(row), float(query.value('lightpctPF')),float_format)

            row += 1
            file.close()

        workbook.close()
        self.db5.close()
        os.system("start EXCEL.EXE " + self.dirYearly+"/"+str(syear)+'_PEA_Yearly_report_'+keytagname[index][0]+'.xlsx')

    def createDailyReport(self):
        
        sdate = date(self.dateTimeStartEdit.date().year(), self.dateTimeStartEdit.date().month(), self.dateTimeStartEdit.date().day())
        edate = date(self.dateTimeFinishEdit.date().year(), self.dateTimeFinishEdit.date().month(), self.dateTimeFinishEdit.date().day())      
        delta = edate - sdate

        for i in range(delta.days + 1):

            if not self.db.open(): 
                self.db.open()

            query = QSqlQuery(self.db)

            day = sdate + timedelta(days=i)
            # print(day.strftime("%Y-%m-%d"))

            index = self.dailyReports.currentIndex()

            row = 9
            # print(keytagname[index][0])
            report_date = day.strftime("%Y-%m-%d")
            excel_date = day.strftime("%Y%m%d")        
            report_date_display =  day.strftime("%d/%m/%Y")            
            yesterday = (sdate - timedelta(1)).strftime('%Y-%m-%d')

            # current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_datetime = datetime.now()
            print(current_datetime)
            
            print(report_date)
            print("setzero --> ",setzero[index])
            print(yesterday)

            workbook = xlsxwriter.Workbook(self.dirDaily+"/"+excel_date+'_PEA_Daily_report_'+keytagname[index][0]+'.xlsx')
            worksheet = workbook.add_worksheet()
            
            worksheet.set_paper(9)
            worksheet.fit_to_pages(1, 0)
            
            border_format=workbook.add_format({'border':1})
            worksheet.conditional_format('A8:K56',{'type':'blanks','format' : border_format} )
            
            worksheet.insert_image('A1', 'pea.png', {'x_scale': 1.25, 'y_scale': 1.25})                
            worksheet.set_column('A:H', 12)
            worksheet.set_column('I:K', 14)
            
            cell_format_header = workbook.add_format({'align': 'center',
                                'valign': 'vcenter',
                                'bold': True, 'font_size': 14})
                            
            worksheet.merge_range('D2:I2', "Daily report", cell_format_header)
            worksheet.merge_range('D3:I3', configSite + " substation", cell_format_header)
            worksheet.merge_range('D4:I4', keytagname[index][11], cell_format_header)
            worksheet.merge_range('D5:I5', "Date: " + str(report_date_display), cell_format_header)         
            
            cell_format_header_data = workbook.add_format({'align': 'center',
                                'valign': 'vcenter',
                                'bold': True, 'font_size': 12,'border':1})                    

            worksheet.write('A8', 'Date', cell_format_header_data)
            worksheet.write('B8', 'Time', cell_format_header_data)
            worksheet.write('C8', 'kV(AB)', cell_format_header_data)
            worksheet.write('D8', 'kV(BC)', cell_format_header_data)
            worksheet.write('E8', 'kV(CA)', cell_format_header_data)
            worksheet.write('F8', 'IA', cell_format_header_data)
            worksheet.write('G8', 'IB', cell_format_header_data)
            worksheet.write('H8', 'IC', cell_format_header_data)
            worksheet.write('I8', 'MW', cell_format_header_data)
            worksheet.write('J8', 'Mvar', cell_format_header_data)
            worksheet.write('K8', '%PF', cell_format_header_data)

            float_format = workbook.add_format({'num_format': '###,###,##0.00', 'font_size': 12,'border':1})
            float_format_datecell = workbook.add_format({'num_format': 'd/m/yyyy', 'font_size': 12,'border':1})
            float_format_timecell = workbook.add_format({'num_format': 'hh:mm:ss','font_size': 12,'border':1})

            query_cmd = 'with ' \
                    'analog_transition_kvab as (' \
                    '    (SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC) ' \
                    '    UNION' \
                    '    (select (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time, NULL,NULL,NULL)' \
                    '),' \
                    'analog_transition_kvbc as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][2])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_kvca as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][3])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ia as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ib as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ic as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_mw as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_mvar as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_pctpf as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_kvab_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ia_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ib_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ic_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_mw_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_mvar_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_pctpf_down as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'first_records as(' \
                    'select' \
                    '    (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVAB,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][2])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVBC,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][3])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVCA,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IA,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IB,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IC,' \
                    '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                    '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][7])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MW,' \
                    '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                    '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][8])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MVar,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][9])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as pctPF' \
                    '),' \
                    'join_records as (' \
                    '    select kvab.start_time,' \
                    '    COALESCE(f_record.kVAB,kvab."Value") as kVAB,' \
                    '    COALESCE(f_record.kVBC,kvbc."Value") as kVBC,' \
                    '    COALESCE(f_record.kVCA,kvca."Value") as kVCA,' \
                    '    COALESCE(f_record.IA,ia."Value") as IA,' \
                    '    COALESCE(f_record.IB,ib."Value") as IB,' \
                    '    COALESCE(f_record.IC,ic."Value") as IC,' \
                    '    COALESCE(f_record.MW,(CASE WHEN ABS(mw."Value") <= 10000 THEN mw."Value"/100 WHEN ABS(mw."Value") > 10000 THEN mw."Value"/1000000 ELSE mw."Value" END)) as MW,' \
                    '    COALESCE(f_record.MVar,(CASE WHEN ABS(mvar."Value") <= 10000 THEN mvar."Value"/100 WHEN ABS(mvar."Value") > 10000 THEN mvar."Value"/1000000 ELSE mvar."Value" END)) as MVar,' \
                    '    COALESCE((CASE WHEN pctpf."Value" > 100 THEN pctpf."Value"/100 WHEN pctpf."Value" < -100 THEN pctpf."Value"/100 ELSE pctpf."Value" END),' \
                    '    (CASE WHEN f_record.pctPF > 100 THEN f_record.pctPF/100 WHEN f_record.pctPF < -100 THEN f_record.pctPF/100 ELSE f_record.pctPF END))as pctPF,' \
                    '    kvab_down."Value" as kVAB_down,' \
                    '    kvab_down."Timestamp" as kVAB_downtime,' \
                    '    kvab_down."start_time" as kVAB_downtime_start_time,' \
                    '    ia_down."Value" as IA_down,' \
                    '    ia_down."Timestamp" as IA_downtime,' \
                    '    ia_down."start_time" as IA_downtime_start_time,' \
                    '    ib_down."Value" as IB_down,' \
                    '    ib_down."Timestamp" as IB_downtime,' \
                    '    ib_down."start_time" as IB_downtime_start_time,' \
                    '    ic_down."Value" as IC_down,' \
                    '    ic_down."Timestamp" as IC_downtime,' \
                    '    ic_down."start_time" as IC_downtime_start_time,' \
                    '    mw_down."Value" as MW_down,' \
                    '    mw_down."Timestamp" as MW_downtime,' \
                    '    mw_down."start_time" as MW_downtime_start_time,' \
                    '    mvar_down."Value" as MVar_down,' \
                    '    mvar_down."Timestamp" as MVar_downtime,' \
                    '    mvar_down."start_time" as MVar_downtime_start_time,' \
                    '    pctpf_down."Value" as pctPF_down,' \
                    '    pctpf_down."Timestamp" as pctPF_downtime,' \
                    '    pctpf_down."start_time" as pctPF_downtime_start_time,' \
                    '    kvab."Timestamp" as kVAB_firstseentime ' \
                    '    from analog_transition_kvab kvab' \
                    '    LEFT JOIN analog_transition_kvbc kvbc ON kvab.start_time = kvbc.start_time' \
                    '    LEFT JOIN analog_transition_kvca kvca ON kvab.start_time = kvca.start_time ' \
                    '    LEFT JOIN analog_transition_ia ia ON kvab.start_time = ia.start_time' \
                    '    LEFT JOIN analog_transition_ib ib ON kvab.start_time = ib.start_time ' \
                    '    LEFT JOIN analog_transition_ic ic ON kvab.start_time = ic.start_time ' \
                    '    LEFT JOIN analog_transition_mw mw ON kvab.start_time = mw.start_time ' \
                    '    LEFT JOIN analog_transition_mvar mvar ON kvab.start_time = mvar.start_time ' \
                    '    LEFT JOIN analog_transition_pctpf pctpf ON kvab.start_time = pctpf.start_time ' \
                    '    LEFT JOIN analog_transition_kvab_down kvab_down ON kvab.start_time = kvab_down.start_time' \
                    '    LEFT JOIN analog_transition_ia_down ia_down ON kvab.start_time = ia_down.start_time' \
                    '    LEFT JOIN analog_transition_ib_down ib_down ON kvab.start_time = ib_down.start_time' \
                    '    LEFT JOIN analog_transition_ic_down ic_down ON kvab.start_time = ic_down.start_time' \
                    '    LEFT JOIN analog_transition_mw_down mw_down ON kvab.start_time = mw_down.start_time' \
                    '    LEFT JOIN analog_transition_mvar_down mvar_down ON kvab.start_time = mvar_down.start_time' \
                    '    LEFT JOIN analog_transition_pctpf_down pctpf_down ON kvab.start_time = pctpf_down.start_time' \
                    '    LEFT JOIN first_records f_record ON kvab.start_time = f_record.start_time' \
                    '    Order by kvab.start_time ASC' \
                    '),' \
                    'prior_ia as (' \
                    '    select start_time,' \
                    '    (select js_sub.IA from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IA is not null ORDER BY js_sub.start_time DESC limit 1) as priorIA' \
                    '    from join_records js where IA IS NULL' \
                    '),' \
                    'prior_ib as (' \
                    '    select start_time,' \
                    '    (select js_sub.IB from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IB is not null ORDER BY js_sub.start_time DESC limit 1) as priorIB' \
                    '    from join_records js where IB IS NULL' \
                    '),' \
                    'prior_ic as (' \
                    '    select start_time,' \
                    '    (select js_sub.IC from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IC is not null ORDER BY js_sub.start_time DESC limit 1) as priorIC' \
                    '    from join_records js where IC IS NULL' \
                    '),' \
                    'prior_mw as (' \
                    '    select start_time,' \
                    '    (select js_sub.MW from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.MW is not null ORDER BY js_sub.start_time DESC limit 1) as priorMW' \
                    '    from join_records js where MW IS NULL' \
                    '),' \
                    'prior_mvar as (' \
                    '    select start_time,' \
                    '    (select js_sub.MVar from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.MVar is not null ORDER BY js_sub.start_time DESC limit 1) as priorMVar' \
                    '    from join_records js where MVar IS NULL' \
                    '),' \
                    'prior_pctpf as (' \
                    '    select start_time,' \
                    '    (select js_sub.pctPF from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.pctPF is not null ORDER BY js_sub.start_time DESC limit 1) as priorpctPF' \
                    '    from join_records js where pctPF IS NULL' \
                    '),' \
                    'result_records as (' \
                    '    select js.start_time,' \
                    '    COALESCE(js.kVAB_down, js.kVAB) as kVAB,' \
                    '    COALESCE(js.kVAB_down, js.kVBC) as kVBC,' \
                    '    COALESCE(js.kVAB_down, js.kVCA) as kVCA,' \
                    '    COALESCE(js.IA_down, COALESCE(js.IA, p_ia.priorIA)) as IA,' \
                    '    COALESCE(js.IB_down, COALESCE(js.IB, p_ib.priorIB)) as IB,' \
                    '    COALESCE(js.IC_down, COALESCE(js.IC, p_ic.priorIC)) as IC,' \
                    '    COALESCE(js.MW_down, COALESCE(js.MW, p_mw.priorMW)) as MW,' \
                    '    COALESCE(js.MVar_down, COALESCE(js.MVar, p_mvar.priorMVar)) as MVar,' \
                    '    COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF)) as pctPF' \
                    '    from join_records js' \
                    '    LEFT JOIN prior_ia p_ia ON js.start_time = p_ia.start_time' \
                    '    LEFT JOIN prior_ib p_ib ON js.start_time = p_ib.start_time' \
                    '    LEFT JOIN prior_ic p_ic ON js.start_time = p_ic.start_time' \
                    '    LEFT JOIN prior_mw p_mw ON js.start_time = p_mw.start_time' \
                    '    LEFT JOIN prior_mvar p_mvar ON js.start_time = p_mvar.start_time' \
                    '    LEFT JOIN prior_pctpf p_prior_pctpf ON js.start_time = p_prior_pctpf.start_time' \
                    '),' \
                    'timeseries as (' \
                    '    select ' \
                    '    (select \''+report_date+'\'::date) + ( n || \' minutes\')::interval start_time ' \
                    '   from generate_series(0, (24*60), 30) n limit 49' \
                    ')' \
                    'select ts.start_time as start_time,rr.kVAB as kVAB,rr.kVBC,rr.kVCA,rr.IA,rr.IB,rr.IC,rr.pctPF,rr.MW,rr.MVar from result_records rr' \
                    ' full join timeseries ts on rr.start_time::timestamp WITHOUT TIME ZONE = ts.start_time  where ts.start_time >= \''+yesterday+' 23:00:00\'' \
                    ' and ts.start_time <= \''+report_date+' 23:59:59.999\' order by ts.start_time ASC'

            print("-----------------------------------")
            print(query_cmd)
            print("-----------------------------------")

            if os.path.exists("queryDaily.log"):
                os.remove("queryDaily.log")
            file = open('queryDaily.log','a+')
            file.write(query_cmd)
            file.close()

            query.exec_(query_cmd)

            while query.next():
                start_date_en = QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "d/M/yyyy")
                start_time_en = QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "hh:mm:ss")
                # print("start_time_en:",start_time_en,"MW",query.value('MW'),"MWPrior:",query.value('MWPrior'),"MWPrior:",priorMW)
                if query.value('start_time') < current_datetime: 

                    worksheet.write('A'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "d/M/yyyy"),float_format_datecell) 
                    worksheet.write('B'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "hh:mm:ss"),float_format_timecell) 
                    if isinstance(query.value('kVAB'), float):
                        worksheet.write_number('C'+ str(row), query.value('kVAB'),float_format)
                    if isinstance(query.value('kVBC'), float) :
                        worksheet.write_number('D'+ str(row), query.value('kVBC'),float_format)
                    if isinstance(query.value('kVCA'), float):
                        worksheet.write_number('E'+ str(row), query.value('kVCA'),float_format)
                    if isinstance(query.value('IA'), float):
                        worksheet.write_number('F'+ str(row), query.value('IA'),float_format)
                    if isinstance(query.value('IB'), float):
                        worksheet.write_number('G'+ str(row), query.value('IB'),float_format)
                    if isinstance(query.value('IC'), float):
                        worksheet.write_number('H'+ str(row), query.value('IC'),float_format)
                    if isinstance(query.value('MW'), float) or isinstance(query.value('MW'), int):
                        worksheet.write_number('I'+ str(row), query.value('MW'),float_format)
                    if isinstance(query.value('MVar'), float) or isinstance(query.value('MVar'), int):
                        worksheet.write_number('J'+ str(row), query.value('MVar'),float_format)
                    if isinstance(query.value('pctPF'), float) :
                        worksheet.write_number('K'+ str(row), query.value('pctPF'),float_format)
                else:

                    worksheet.write('A'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "d/M/yyyy"),float_format_datecell) 
                    worksheet.write('B'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "hh:mm:ss"),float_format_timecell) 

                row += 1


            workbook.close()
            self.db.close()
            os.system("start EXCEL.EXE " + self.dirDaily+"/"+excel_date+'_PEA_Daily_report_'+keytagname[index][0]+'.xlsx')


class Thread(QThread):
    
    def __init__(self, parent):
        
        QThread.__init__(self, parent)
        self.window = parent
        self._lock = threading.Lock()
        self.running = False

    def stop(self):
        self.running = False
        with self._lock:
            self._do_before_done()
        app.quit()

    def stop2(self):
        self.running = False
        
    def start2(self):
        self.running = True
        # self.terminate()        

    def update_crontime(self):
        configCrontime = settings.value('Output/crontime', "23:45:00")

    def _do_work(self):
        configCrontime = settings.value('Output/crontime', "")
        now = datetime.now()
        now_time = now.strftime("%H:%M")
        cron_time_string = configCrontime        
        cron_timex = datetime.strptime(cron_time_string, "%H:%M:%S")
        cron_time = cron_timex.strftime("%H:%M")
        # cron_time = now.replace(hour=cron_time.time().hour, minute=cron_time.time().minute, second=cron_time.time().second, microsecond=0)
        if (now_time == cron_time):           
            self.createDailyReportbyCrontab()
        else:
            self.sleep(5)

    def createDailyReportbyCrontab(self):

        self.db2 = QSqlDatabase.addDatabase('QPSQL','ThreadConnect')
        self.db2.setHostName(configHost)
        self.db2.setDatabaseName(configDatabase)
        self.db2.setPort(int(configPort))
        self.db2.setUserName(configUsername)
        self.db2.setPassword(configPassword)
        ok = self.db2.open()
        if not ok:
            print("Thread Connection Error: " + str(self.db2.lastError().text()))

        self.dirDaily = configOutput+"/Daily"

        report_date = datetime.today().strftime('%Y-%m-%d')
        excel_date = datetime.today().strftime('%Y%m%d')
        report_date_display = datetime.today().strftime("%d/%m/%Y")
        gendate_yesterday = datetime.today() - timedelta(1)
        yesterday = datetime.strftime(gendate_yesterday, '%Y-%m-%d')

        # current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_datetime = datetime.now()
        print(current_datetime)

        # yesterday = "2021-02-03"
        # report_date = "2021-02-04"
        # excel_date = "20210204"
        # report_date_display = "04/02/2021"

        for j in range(len(keytagname)):

            print(setzero[j])
            row = 9            
            workbook = xlsxwriter.Workbook(self.dirDaily+"/"+excel_date+'_PEA_Daily_report_'+keytagname[j][0]+'.xlsx')
            worksheet = workbook.add_worksheet()
            worksheet.set_paper(9)
            worksheet.fit_to_pages(1, 0)
            
            border_format=workbook.add_format({'border':1})
            worksheet.conditional_format('A8:K56',{'type':'blanks','format' : border_format} )

            float_format = workbook.add_format({'num_format': '###,###,##0.00', 'font_size': 12,'border':1})
            float_format_datecell = workbook.add_format({'num_format': 'd/m/yyyy', 'font_size': 12,'border':1})
            float_format_timecell = workbook.add_format({'num_format': 'hh:mm:ss','font_size': 12,'border':1})         

            cell_format_header_data = workbook.add_format({'align': 'center',
                                'valign': 'vcenter',
                                'bold': True, 'font_size': 12,'border':1})     
            cell_format_header = workbook.add_format({'align': 'center',
                                'valign': 'vcenter',
                                'bold': True, 'font_size': 14})

            worksheet.insert_image('A1', 'pea.png',{'x_scale': 1.25, 'y_scale': 1.25})                
            worksheet.set_column('A:H', 12)
            worksheet.set_column('I:K', 14)
                                        
            worksheet.merge_range('D2:I2', "Daily report", cell_format_header)
            worksheet.merge_range('D3:I3', configSite + " substation", cell_format_header)
            worksheet.merge_range('D4:I4', keytagname[j][11], cell_format_header)
            worksheet.merge_range('D5:I5', "Date: " + str(report_date_display), cell_format_header)        
            
            worksheet.write('A8', 'Date', cell_format_header_data)
            worksheet.write('B8', 'Time', cell_format_header_data)
            worksheet.write('C8', 'kV(AB)', cell_format_header_data)
            worksheet.write('D8', 'kV(BC)', cell_format_header_data)
            worksheet.write('E8', 'kV(CA)', cell_format_header_data)
            worksheet.write('F8', 'IA', cell_format_header_data)
            worksheet.write('G8', 'IB', cell_format_header_data)
            worksheet.write('H8', 'IC', cell_format_header_data)
            worksheet.write('I8', 'MW', cell_format_header_data)
            worksheet.write('J8', 'Mvar', cell_format_header_data)
            worksheet.write('K8', '%PF', cell_format_header_data)

            query = QSqlQuery(self.db2)

            # // crontab
            query_cmd = 'with ' \
                    'analog_transition_kvab as (' \
                    '    (SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][1])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC) ' \
                    '    UNION' \
                    '    (select (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time, NULL,NULL,NULL)' \
                    '),' \
                    'analog_transition_kvbc as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][2])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_kvca as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][3])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ia as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][4])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ib as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][5])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_ic as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][6])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_mw as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][7])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_mvar as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][8])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_pctpf as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[j][9])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" is not null' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC, ant."TimestampIndex" DESC' \
                    '),' \
                    'analog_transition_kvab_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][1])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ia_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][4])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ib_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][5])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_ic_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][6])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_mw_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][7])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_mvar_down as (' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][8])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\'  ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'analog_transition_pctpf_down as ( ' \
                    '    SELECT DISTINCT ON ((date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\')' \
                    '    (date_trunc(\'hour\', ant."Timestamp") + date_part(\'minute\', ant."Timestamp")::int / 30 * interval \'30 min\') + interval \'30 min\' start_time,' \
                    '    ant."Timestamp",ant."Value",ant."TimestampIndex"' \
                    '    from "AnalogTransition" ant ' \
                    '    where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[j][9])+')' \
                    '    and ant."Timestamp" >= \''+yesterday+' 23:00:00\' ' \
                    '    and ant."Timestamp" <= \''+report_date+' 23:59:59.999\' ' \
                    '    and ant."Value" = \'0.00\'' \
                    '    ORDER BY start_time ASC, ant."Timestamp" DESC ' \
                    '),' \
                    'first_records as(' \
                    'select' \
                    '    (\'2000-01-01 00:00:00\'::timestamp with time zone) start_time,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][1])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVAB,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][2])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVBC,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][3])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVCA,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][4])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IA,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][5])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IB,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][6])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IC,' \
                    '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                    '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][7])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MW,' \
                    '    (select (CASE WHEN ABS(ant."Value") <= 10000 THEN ant."Value"/100 WHEN ABS(ant."Value") > 10000 THEN ant."Value"/1000000 ELSE ant."Value" END)' \
                    '    from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][8])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MVar,' \
                    '    (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[j][9])+')' \
                    '    and ant."Timestamp" < \''+yesterday+' 23:00:00\' and ant."Value" is not null' \
                    '    ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as pctPF' \
                    '),' \
                    'join_records as (' \
                    '    select kvab.start_time,' \
                    '    COALESCE(f_record.kVAB,kvab."Value") as kVAB,' \
                    '    COALESCE(f_record.kVBC,kvbc."Value") as kVBC,' \
                    '    COALESCE(f_record.kVCA,kvca."Value") as kVCA,' \
                    '    COALESCE(f_record.IA,ia."Value") as IA,' \
                    '    COALESCE(f_record.IB,ib."Value") as IB,' \
                    '    COALESCE(f_record.IC,ic."Value") as IC,' \
                    '    COALESCE(f_record.MW,(CASE WHEN ABS(mw."Value") <= 10000 THEN mw."Value"/100 WHEN ABS(mw."Value") > 10000 THEN mw."Value"/1000000 ELSE mw."Value" END)) as MW,' \
                    '    COALESCE(f_record.MVar,(CASE WHEN ABS(mvar."Value") <= 10000 THEN mvar."Value"/100 WHEN ABS(mvar."Value") > 10000 THEN mvar."Value"/1000000 ELSE mvar."Value" END)) as MVar,' \
                    '    COALESCE((CASE WHEN pctpf."Value" > 100 THEN pctpf."Value"/100 WHEN pctpf."Value" < -100 THEN pctpf."Value"/100 ELSE pctpf."Value" END),' \
                    '    (CASE WHEN f_record.pctPF > 100 THEN f_record.pctPF/100 WHEN f_record.pctPF < -100 THEN f_record.pctPF/100 ELSE f_record.pctPF END))as pctPF,' \
                    '    kvab_down."Value" as kVAB_down,' \
                    '    kvab_down."Timestamp" as kVAB_downtime,' \
                    '    kvab_down."start_time" as kVAB_downtime_start_time,' \
                    '    ia_down."Value" as IA_down,' \
                    '    ia_down."Timestamp" as IA_downtime,' \
                    '    ia_down."start_time" as IA_downtime_start_time,' \
                    '    ib_down."Value" as IB_down,' \
                    '    ib_down."Timestamp" as IB_downtime,' \
                    '    ib_down."start_time" as IB_downtime_start_time,' \
                    '    ic_down."Value" as IC_down,' \
                    '    ic_down."Timestamp" as IC_downtime,' \
                    '    ic_down."start_time" as IC_downtime_start_time,' \
                    '    mw_down."Value" as MW_down,' \
                    '    mw_down."Timestamp" as MW_downtime,' \
                    '    mw_down."start_time" as MW_downtime_start_time,' \
                    '    mvar_down."Value" as MVar_down,' \
                    '    mvar_down."Timestamp" as MVar_downtime,' \
                    '    mvar_down."start_time" as MVar_downtime_start_time,' \
                    '    pctpf_down."Value" as pctPF_down,' \
                    '    pctpf_down."Timestamp" as pctPF_downtime,' \
                    '    pctpf_down."start_time" as pctPF_downtime_start_time,' \
                    '    kvab."Timestamp" as kVAB_firstseentime ' \
                    '    from analog_transition_kvab kvab' \
                    '    LEFT JOIN analog_transition_kvbc kvbc ON kvab.start_time = kvbc.start_time' \
                    '    LEFT JOIN analog_transition_kvca kvca ON kvab.start_time = kvca.start_time ' \
                    '    LEFT JOIN analog_transition_ia ia ON kvab.start_time = ia.start_time' \
                    '    LEFT JOIN analog_transition_ib ib ON kvab.start_time = ib.start_time ' \
                    '    LEFT JOIN analog_transition_ic ic ON kvab.start_time = ic.start_time ' \
                    '    LEFT JOIN analog_transition_mw mw ON kvab.start_time = mw.start_time ' \
                    '    LEFT JOIN analog_transition_mvar mvar ON kvab.start_time = mvar.start_time ' \
                    '    LEFT JOIN analog_transition_pctpf pctpf ON kvab.start_time = pctpf.start_time ' \
                    '    LEFT JOIN analog_transition_kvab_down kvab_down ON kvab.start_time = kvab_down.start_time' \
                    '    LEFT JOIN analog_transition_ia_down ia_down ON kvab.start_time = ia_down.start_time' \
                    '    LEFT JOIN analog_transition_ib_down ib_down ON kvab.start_time = ib_down.start_time' \
                    '    LEFT JOIN analog_transition_ic_down ic_down ON kvab.start_time = ic_down.start_time' \
                    '    LEFT JOIN analog_transition_mw_down mw_down ON kvab.start_time = mw_down.start_time' \
                    '    LEFT JOIN analog_transition_mvar_down mvar_down ON kvab.start_time = mvar_down.start_time' \
                    '    LEFT JOIN analog_transition_pctpf_down pctpf_down ON kvab.start_time = pctpf_down.start_time' \
                    '    LEFT JOIN first_records f_record ON kvab.start_time = f_record.start_time' \
                    '    Order by kvab.start_time ASC' \
                    '),' \
                    'prior_ia as (' \
                    '    select start_time,' \
                    '    (select js_sub.IA from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IA is not null ORDER BY js_sub.start_time DESC limit 1) as priorIA' \
                    '    from join_records js where IA IS NULL' \
                    '),' \
                    'prior_ib as (' \
                    '    select start_time,' \
                    '    (select js_sub.IB from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IB is not null ORDER BY js_sub.start_time DESC limit 1) as priorIB' \
                    '    from join_records js where IB IS NULL' \
                    '),' \
                    'prior_ic as (' \
                    '    select start_time,' \
                    '    (select js_sub.IC from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.IC is not null ORDER BY js_sub.start_time DESC limit 1) as priorIC' \
                    '    from join_records js where IC IS NULL' \
                    '),' \
                    'prior_mw as (' \
                    '    select start_time,' \
                    '    (select js_sub.MW from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.MW is not null ORDER BY js_sub.start_time DESC limit 1) as priorMW' \
                    '    from join_records js where MW IS NULL' \
                    '),' \
                    'prior_mvar as (' \
                    '    select start_time,' \
                    '    (select js_sub.MVar from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.MVar is not null ORDER BY js_sub.start_time DESC limit 1) as priorMVar' \
                    '    from join_records js where MVar IS NULL' \
                    '),' \
                    'prior_pctpf as (' \
                    '    select start_time,' \
                    '    (select js_sub.pctPF from join_records js_sub where' \
                    '    js_sub.start_time < js.start_time and js_sub.pctPF is not null ORDER BY js_sub.start_time DESC limit 1) as priorpctPF' \
                    '    from join_records js where pctPF IS NULL' \
                    '),' \
                    'result_records as (' \
                    '    select js.start_time,' \
                    '    COALESCE(js.kVAB_down, js.kVAB) as kVAB,' \
                    '    COALESCE(js.kVAB_down, js.kVBC) as kVBC,' \
                    '    COALESCE(js.kVAB_down, js.kVCA) as kVCA,' \
                    '    COALESCE(js.IA_down, COALESCE(js.IA, p_ia.priorIA)) as IA,' \
                    '    COALESCE(js.IB_down, COALESCE(js.IB, p_ib.priorIB)) as IB,' \
                    '    COALESCE(js.IC_down, COALESCE(js.IC, p_ic.priorIC)) as IC,' \
                    '    COALESCE(js.MW_down, COALESCE(js.MW, p_mw.priorMW)) as MW,' \
                    '    COALESCE(js.MVar_down, COALESCE(js.MVar, p_mvar.priorMVar)) as MVar,' \
                    '    COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF)) as pctPF' \
                    '    from join_records js' \
                    '    LEFT JOIN prior_ia p_ia ON js.start_time = p_ia.start_time' \
                    '    LEFT JOIN prior_ib p_ib ON js.start_time = p_ib.start_time' \
                    '    LEFT JOIN prior_ic p_ic ON js.start_time = p_ic.start_time' \
                    '    LEFT JOIN prior_mw p_mw ON js.start_time = p_mw.start_time' \
                    '    LEFT JOIN prior_mvar p_mvar ON js.start_time = p_mvar.start_time' \
                    '    LEFT JOIN prior_pctpf p_prior_pctpf ON js.start_time = p_prior_pctpf.start_time' \
                    '),' \
                    'timeseries as (' \
                    '    select ' \
                    '    (select \''+report_date+'\'::date) + ( n || \' minutes\')::interval start_time ' \
                    '   from generate_series(0, (24*60), 30) n limit 49' \
                    ')' \
                    'select ts.start_time as start_time,rr.kVAB as kVAB,rr.kVBC,rr.kVCA,rr.IA,rr.IB,rr.IC,rr.pctPF,rr.MW,rr.MVar from result_records rr' \
                    ' full join timeseries ts on rr.start_time::timestamp WITHOUT TIME ZONE = ts.start_time  where ts.start_time >= \''+yesterday+' 23:00:00\'' \
                    ' and ts.start_time <= \''+report_date+' 23:59:59.999\' order by ts.start_time ASC'


            query.exec_(query_cmd)
            while query.next():

                start_date_en = QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "d/M/yyyy")
                start_time_en = QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "hh:mm:ss")

                if query.value('start_time') < current_datetime: 

                    worksheet.write('A'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "d/M/yyyy"),float_format_datecell) 
                    worksheet.write('B'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "hh:mm:ss"),float_format_timecell) 

                    if isinstance(query.value('kVAB'), float):
                        worksheet.write_number('C'+ str(row), query.value('kVAB'),float_format)
                    if isinstance(query.value('kVBC'), float) :
                        worksheet.write_number('D'+ str(row), query.value('kVBC'),float_format)
                    if isinstance(query.value('kVCA'), float):
                        worksheet.write_number('E'+ str(row), query.value('kVCA'),float_format)
                    if isinstance(query.value('IA'), float):
                        worksheet.write_number('F'+ str(row), query.value('IA'),float_format)
                    if isinstance(query.value('IB'), float):
                        worksheet.write_number('G'+ str(row), query.value('IB'),float_format)
                    if isinstance(query.value('IC'), float):
                        worksheet.write_number('H'+ str(row), query.value('IC'),float_format)
                    if isinstance(query.value('MW'), float) or isinstance(query.value('MW'), int):
                        worksheet.write_number('I'+ str(row), query.value('MW'),float_format)
                    if isinstance(query.value('MVar'), float) or isinstance(query.value('MVar'), int):
                        worksheet.write_number('J'+ str(row), query.value('MVar'),float_format)
                    if isinstance(query.value('pctPF'), float) :
                        worksheet.write_number('K'+ str(row), query.value('pctPF'),float_format)
                else:
                    worksheet.write('A'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "d/M/yyyy"),float_format_datecell) 
                    worksheet.write('B'+ str(row), QLocale(QLocale.English, QLocale.UnitedStates).toString(query.value('start_time'), "hh:mm:ss"),float_format_timecell) 

                row += 1

            workbook.close()

        self.db2.close()
        self.sleep(60)

    def _do_before_done(self):
        # print('ok, thread stop done.')
        return
    
    def run(self):
        self.running = True
        while self.running:
            with self._lock:
                self._do_work()

if __name__ == '__main__':
    try:
        app = QApplication(sys.argv)
        lock_file = QLockFile("app.lock")

        if lock_file.tryLock():
            app.setWindowIcon(QIcon('report.ico'))
            window = MainWindow()
            window.show()
            sys.exit(app.exec_())
        else:
            error_message = QMessageBox()
            error_message.setIcon(QMessageBox.Warning)
            error_message.setWindowTitle("Error")
            error_message.setText("The application is already running!")
            error_message.setStandardButtons(QMessageBox.Ok)
            error_message.exec()
    finally:
        lock_file.unlock()
