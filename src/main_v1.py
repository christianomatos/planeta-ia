"""
main_v1.py - Detecção de mãos usando MediaPipe Hands e OpenCV

Este script abre a webcam, detecta mãos em tempo real usando MediaPipe
e desenha os landmarks (pontos de referência) nas mãos detectadas.
Pressione 'q' para fechar a janela.

Requisitos:
    - opencv-python
    - mediapipe

Instalação:
    pip install opencv-python mediapipe
"""

import cv2
import mediapipe as mp


def main():
    # Inicializa o MediaPipe Hands
    mp_hands = mp.solutions.hands
    hands = mp_hands.Hands(
        static_image_mode=False,  # Detecta mãos em cada frame
        max_num_hands=2,          # Número máximo de mãos para detectar
        min_detection_confidence=0.5,  # Confiança mínima para detecção
        min_tracking_confidence=0.5     # Confiança mínima para rastreamento
    )
    
    # Inicializa o utilitário de desenho
    mp_drawing = mp.solutions.drawing_utils
    
    # Abre a webcam (0 = câmera padrão)
    cap = cv2.VideoCapture(0)
    
    if not cap.isOpened():
        print("Erro: Não foi possível abrir a webcam.")
        return
    
    print("Webcam aberta. Pressione 'q' para sair.")
    
    while True:
        # Captura frame por frame
        ret, frame = cap.read()
        
        if not ret:
            print("Erro: Não foi possível capturar o frame.")
            break
        
        # Converte a imagem BGR (OpenCV) para RGB (MediaPipe)
        image = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        
        # Processa a imagem e detecta as mãos
        results = hands.process(image)
        
        # Se mãos forem detectadas, desenha os landmarks
        if results.multi_hand_landmarks:
            for hand_landmarks in results.multi_hand_landmarks:
                # Desenha os landmarks e conexões na mão
                mp_drawing.draw_landmarks(
                    frame,
                    hand_landmarks,
                    mp_hands.HAND_CONNECTIONS,
                    # Personaliza o desenho dos landmarks (opcional)
                    mp_drawing.DrawingSpec(
                        color=(0, 255, 0),      # Cor verde para landmarks
                        thickness=2,
                        circle_radius=3
                    ),
                    mp_drawing.DrawingSpec(
                        color=(255, 0, 0),      # Cor azul para conexões
                        thickness=2
                    )
                )
        
        # Exibe o frame resultante
        cv2.imshow('Detecção de Mãos - MediaPipe', frame)
        
        # Verifica se a tecla 'q' foi pressionada
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    
    # Libera os recursos
    cap.release()
    cv2.destroyAllWindows()
    hands.close()
    
    print("Aplicação fechada.")


if __name__ == "__main__":
    main()