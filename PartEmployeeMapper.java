package Sy52;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartEmployeeMapper extends Mapper<LongWritable, Text, IntWritable, Employee> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException{
        //数据
        String data = value1.toString();
        //分词
        String[] words = data.split(",");
        //创建员工对象
        Employee e = new Employee();
        //设置员工属性
        //员工号
        e.setEmpno(Integer.parseInt(words[0]));
        //姓名
        e.setEname(words[1]);
        //职位
        e.setJob(words[2]);
        //老板号（可能没有老板号）
        try{
            e.getMgr(Integer.parseInt(words[3]));
        }catch (Exception ex){
            //没有老板号
            e.setMgr(-1);
        }
        //入职日期
        e.setHiredate(words[4]);
        //月薪
        e.setSal(Integer.parseInt(words[5]));
        //奖金（可能没有奖金）
        try{
            e.getComm(Integer.parseInt(words[6]));
        }catch (Exception ex){
            //没有奖金
            e.setComm(0);
        }
        //部门号
        e.setDeptno(Integer.parseInt(words[7]));
        //输出 k2    v2 员工对象
        context.write(new IntWritable(e.getSal()),  //员工月薪
                e);  //员工对象
    }
}
