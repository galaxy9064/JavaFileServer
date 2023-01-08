package serverA;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: XYF
 * @Date: 2023/1/8 19:12
 * @Content: 删除文件
 */
public class FileDeleter {
    ArrayList<String> filePathList;

    public FileDeleter(ArrayList<String> filePathList){
        this.filePathList=filePathList;
    }
    public void deleteFiles(){
        //删除处理过的文件
        for(String filePath:filePathList){
            //删除文件
            boolean delete = new File(filePath).delete();
            System.out.println(new Date(System.currentTimeMillis()) +
                    " [FileDeleter.deleteFiles.INFO]: 删除文件结果："+delete);
        }
    }

}
