pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                checkout([$class: 'GitSCM', branches: [[name: '*/dev']], extensions: [], userRemoteConfigs: [[credentialsId: 'ObjDetctionSegmentation', url: 'https://github.com/omolewadavids/Object_Detection_and_Segmentation.git']]])
            }
        }
        stage('Build') {
            steps{
                git branch: 'dev', credentialsId: 'ObjDetctionSegmentation', url: 'https://github.com/omolewadavids/Object_Detection_and_Segmentation.git'
            }
        }
        stage('Test') {
            steps{
                echo 'Done building and checking for the filex'
                echo 'Omolewa'
            }
        }
    }
}