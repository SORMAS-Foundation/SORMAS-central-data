node {
    
    def VERSION=''
        
    stage('checkout') {
        git branch: '${BRANCH}', url: 'https://github.com/hzi-braunschweig/SORMAS-central-data.git'
        steps {
            // Use a script block to do custom scripting
            script {
                def props = readProperties file: '.env'
                env.VERSION = props.VERSION
            }

            sh "echo The version is $VERSION"        
    }

    stage('Build container') {
    	echo 'Building align-local-central'
    	sh """
    	source ./.env
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