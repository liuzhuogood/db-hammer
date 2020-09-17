# import platform
#
# import jpype
#
#
# class HurbaoTool:
#     jvmPath = jpype.getDefaultJVMPath()
#     jarPath = "-Djava.class.path=hrb-common_jm.jar"
#     if platform.system().lower() == 'windows':
#         jvmPath = r'C:\Program Files\Java\jdk1.8.0_131\jre\bin\server\jvm.dll'
#         jarPath = "-Djava.class.path=D:\\hrb-common_jm.jar"
#     jpype.startJVM(jvmPath, jarPath)
#     jpype.java.lang.System.out.println("hello world!")
#     DefaultSeedAES = jpype.JClass("com.hurbao.utils.secret.DefaultSeedAES")
