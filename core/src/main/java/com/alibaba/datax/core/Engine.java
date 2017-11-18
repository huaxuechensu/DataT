package com.alibaba.datax.core;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.plugin.writer.logrecorder.LogRecorder;
import com.alibaba.datax.plugin.writer.logrecorder.util.MD5Util;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    private static String RUNTIME_MODE;

    /* check job model (job/task) first */
    public void start(Configuration allConf,String serialNum) {

        // 绑定column转换信息
        ColumnCast.bind(allConf);

        /**
         * 初始化PluginLoader，可以获取各种插件配置
         */
        LoadUtil.bind(allConf);

        boolean isJob = !("taskGroup".equalsIgnoreCase(allConf
                .getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));
        //JobContainer会在schedule后再行进行设置和调整值
        int channelNumber =0;
        AbstractContainer container;
        long instanceId;
        int taskGroupId = -1;
        if (isJob) {
            allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);
            container = new JobContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);

        } else {
            container = new TaskGroupContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
            taskGroupId = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            channelNumber = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
        }

        //缺省打开perfTrace
        boolean traceEnable = allConf.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE, true);
        boolean perfReportEnable = allConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG, true);

        //standlone模式的datax shell任务不进行汇报
        if(instanceId == -1){
            perfReportEnable = false;
        }

        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        }catch (NumberFormatException e){
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: "+System.getProperty("PROIORY"));
        }

        Configuration jobInfoConfig = allConf.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO);
        //初始化PerfTrace
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig,perfReportEnable,channelNumber);

        //-----------------------下面部分为后期添加，作用：把执行日志写入日志数据库------------------
        String timeStr = MD5Util.getCurrentTime();
        List<String> renderedPreSqls = new ArrayList<String>();
        String executeSqlStr = "INSERT INTO job_start_info VALUES ('"+serialNum+"','"+isJob+"','"+channelNumber+"','"+instanceId+"','"+taskGroupId+"','"+priority+"','"+traceEnable+"','"+perfReportEnable+"','"+timeStr+"')";
        renderedPreSqls.add(executeSqlStr);
        LogRecorder.insertExecuLog2DB("job_start_info",renderedPreSqls);

        //传递Job最开始的序列号
        container.setDefinedJobId(serialNum);
        //-----------------------上面部分为后期添加，作用：把执行日志写入日志数据库------------------

        container.start();
    }


    // 注意屏蔽敏感信息
    public static String filterJobConfiguration(final Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content",jobContent);

        return jobConfWithSetting.beautify();
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration){
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
        return configuration;
    }

    public static void entry(final String[] args) throws Throwable {
        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("mode", true, "Job runtime mode.");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);

        String jobPath = cl.getOptionValue("job");

        // 如果用户没有明确指定jobid, 则 datax.py 会指定 jobid 默认值为-1
        String jobIdString = cl.getOptionValue("jobid");
        RUNTIME_MODE = cl.getOptionValue("mode");

        Configuration configuration = ConfigParser.parse(jobPath);

        long jobId;
        if (!"-1".equalsIgnoreCase(jobIdString)) {
            jobId = Long.parseLong(jobIdString);
        } else {
            // only for dsc & ds & datax 3 update
            String dscJobUrlPatternString = "/instance/(\\d{1,})/config.xml";
            String dsJobUrlPatternString = "/inner/job/(\\d{1,})/config";
            String dsTaskGroupUrlPatternString = "/inner/job/(\\d{1,})/taskGroup/";
            List<String> patternStringList = Arrays.asList(dscJobUrlPatternString,
                    dsJobUrlPatternString, dsTaskGroupUrlPatternString);
            jobId = parseJobIdFromUrl(patternStringList, jobPath);
        }

        boolean isStandAloneMode = "standalone".equalsIgnoreCase(RUNTIME_MODE);
        if (!isStandAloneMode && jobId == -1) {
            // 如果不是 standalone 模式，那么 jobId 一定不能为-1
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "非 standalone 模式必须在 URL 中提供有效的 jobId.");
        }
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);

        //打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            LOG.info(vmInfo.toString());
        }

        LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");

        LOG.debug(configuration.toJSON());

        ConfigurationValidate.doValidate(configuration);


        //-----------------------下面部分为后期添加，作用：把执行日志写入日志数据库------------------
        String serialNum = MD5Util.getTime5RanNum();
        String timeStr = MD5Util.getCurrentTime();
        String confStr = configuration.toJSON();
        List<String> renderedPreSqls = new ArrayList<String>();
        String executeSqlStr = "INSERT INTO job_entry_info VALUES ('"+serialNum+"','"+jobId+"','"+jobPath+"','"+RUNTIME_MODE+"','"+confStr+"','"+timeStr+"')";
        renderedPreSqls.add(executeSqlStr);
        LogRecorder.insertExecuLog2DB("job_entry_info",renderedPreSqls);
        //-----------------------上面部分为后期添加，作用：把执行日志写入日志数据库------------------

        Engine engine = new Engine();
        engine.start(configuration,serialNum);
    }


    /**
     * -1 表示未能解析到 jobId
     *
     *  only for dsc & ds & datax 3 update
     */
    private static long parseJobIdFromUrl(List<String> patternStringList, String url) {
        long result = -1;
        for (String patternString : patternStringList) {
            result = doParseJobIdFromUrl(patternString, url);
            if (result != -1) {
                return result;
            }
        }
        return result;
    }

    private static long doParseJobIdFromUrl(String patternString, String url) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }

        return -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
       /*  Test
        String[] test = new String[]{"-mode","standalone","-jobid","-1","-job","C:\\Users\\YQ\\Documents\\mysql2mysql.json"};
        args = test;
        String test = "{\"common\":{\"column\":{\"dateFormat\":\"yyyy-MM-dd\",\"datetimeFormat\":\"yyyy-MM-dd HH:mm:ss\",\"encoding\":\"utf-8\",\"extraFormats\":[\"yyyyMMdd\"],\"timeFormat\":\"HH:mm:ss\",\"timeZone\":\"GMT+8\"}},\"core\":{\"container\":{\"job\":{\"id\":0,\"mode\":\"standalone\",\"reportInterval\":10000},\"taskGroup\":{\"channel\":5},\"trace\":{\"enable\":\"false\"}},\"dataXServer\":{\"address\":\"http://localhost:7001/api\",\"reportDataxLog\":false,\"reportPerfLog\":false,\"timeout\":10000},\"statistics\":{\"collector\":{\"plugin\":{\"maxDirtyNumber\":10,\"taskClass\":\"com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector\"}}},\"transport\":{\"channel\":{\"byteCapacity\":67108864,\"capacity\":512,\"class\":\"com.alibaba.datax.core.transport.channel.memory.MemoryChannel\",\"flowControlInterval\":20,\"speed\":{\"byte\":-1,\"record\":-1}},\"exchanger\":{\"bufferSize\":32,\"class\":\"com.alibaba.datax.core.plugin.BufferedRecordExchanger\"}}},\"entry\":{\"jvm\":\"-Xms1G -Xmx1G\"},\"job\":{\"content\":[{\"reader\":{\"name\":\"mysqlreader\",\"parameter\":{\"column\":[\"*\"],\"connection\":[{\"jdbcUrl\":[\"jdbc:mysql://192.168.189.188:3306/test_source\"],\"table\":[\"web_sales\"]}],\"password\":\"root\",\"username\":\"root\",\"where\":\"\"}},\"writer\":{\"name\":\"mysqlwriter\",\"parameter\":{\"column\":[\"*\"],\"connection\":[{\"jdbcUrl\":\"jdbc:mysql://192.168.189.188:3306/test_target\",\"table\":[\"web_sales\"]}],\"password\":\"root\",\"preSql\":[],\"session\":[],\"username\":\"root\",\"writeMode\":\"update\"}}}],\"setting\":{\"speed\":{\"channel\":\"10\"}}},\"plugin\":{\"reader\":{\"mysqlreader\":{\"class\":\"com.alibaba.datax.plugin.reader.mysqlreader.MysqlReader\",\"description\":\"useScene: prod. mechanism: Jdbc connection using the database, execute select sql, retrieve data from the ResultSet. warn: The more you know about the database, the less problems you encounter.\",\"developer\":\"alibaba\",\"name\":\"mysqlreader\",\"path\":\"/opt/datax/plugin/reader/mysqlreader\"}},\"writer\":{\"mysqlwriter\":{\"class\":\"com.alibaba.datax.plugin.writer.mysqlwriter.MysqlWriter\",\"description\":\"useScene: prod. mechanism: Jdbc connection using the database, execute insert sql. warn: The more you know about the database, the less problems you encounter.\",\"developer\":\"alibaba\",\"name\":\"mysqlwriter\",\"path\":\"/opt/datax/plugin/writer/mysqlwriter\"}}}}";
        */

        try {
            Engine.entry(args);
        } catch (Throwable e) {
            exitCode = 1;
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));

            if (e instanceof DataXException) {
                DataXException tempException = (DataXException) e;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }

            System.exit(exitCode);
        }
        System.exit(exitCode);
    }

}
