package bean;

/**
  * 异常模型ID
  */
public enum AnalsModelEnum{

  /** 白名单 */
  WHITE_LIST(1L),

  /** 异地登录 */
  OFFSITE_LOGIN(2L),

  /** 异常时间访问 */
  vABNORMAL_TIME_ACCESS(3L),

  /** 登录失败 */
  LOGIN_FAIL(4L),

  /** 敏感资源下载 */
  SENSITIVE_RESOURCES_DOWNLOAD(5L);

  private long value;

  private AnalsModelEnum(long value){
    this.value = value;
  }

  public Long value() {
    return value;
  }

}
