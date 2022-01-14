node {
    
    def VERSION=''
        
    stage('checkout') {
        git branch: '${BRANCH}', url: 'https://github.com/hzi-braunschweig/SORMAS-central-data.git'
    }

    stage('Build container') {
    	echo 'Building central-aligner'
    	sh """
    	source ./.env
    	sudo docker build --no-cache -t central_aligner:${VERSION} .
    	"""
    }
      
    stage('Push image into registry') {
    echo 'Deploying....'
        withCredentials([ usernamePassword(credentialsId: 'registry.netzlink.com', usernameVariable: 'MY_SECRET_USER_NLI', passwordVariable: 'MY_SECRET_USER_PASSWORD_NLI' )]) {
        	sh """
        	sudo docker login -u '$MY_SECRET_USER_NLI' -p '$MY_SECRET_USER_PASSWORD_NLI' registry.netzlink.com
            sudo docker tag central_aligner:${VERSION} registry.netzlink.com/hzibraunschweig/central-aligner:${VERSION}
        	sudo docker push registry.netzlink.com/hzibraunschweig/central-aligner:${VERSION}
        	"""
        }    
	}
}