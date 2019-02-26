package chapter2;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 代码清单2-3中的Company类
 * Created by 朱小厮 on 2018/7/26.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {
    private String name;
    private String address;
//    private String telphone;
}
