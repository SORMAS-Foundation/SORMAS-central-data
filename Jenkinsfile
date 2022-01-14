node {
        
    stage('checkout') {
        git branch: '${BRANCH}', url: 'https://github.com/hzi-braunschweig/SORMAS-central-data.git'
    }

    stage('reading from a file') {
        script {
            def props = readProperties file: '.env'
            env.VERSION = props.VERSION
        }
    }
    stage('Build container') {
    	echo 'Building align-local-central'
    	sh """
    	sudo buildah rmi \$(buildah images -q localhost/central-aligner)
    	sudo buildah bud --pull-always --no-cache -t central-aligner:${VERSION} .
    	"""
    }
      
    stage('Push image into registry') {
    echo 'Deploying....'
        withCredentials([ usernamePassword(credentialsId: 'registry.netzlink.com', usernameVariable: 'MY_SECRET_USER_NLI', passwordVariable: 'MY_SECRET_USER_PASSWORD_NLI' )]) {
        	sh """
        	sudo buildah login -u '$MY_SECRET_USER_NLI' -p '$MY_SECRET_USER_PASSWORD_NLI' registry.netzlink.com
        	sudo buildah push -f v2s2 central-aligner:${VERSION} registry.netzlink.com/hzibraunschweig/central-aligner:${VERSION}
        	"""
        }    
	}
}